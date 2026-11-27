# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for RTATransfertool.

The NATS bus is mocked. Job bytes are checked against the vendored v1 schema.
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from rucio.common.constants import RseAttr
from rucio.common.exception import RucioException, TransferToolWrongAnswer
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.request import DirectTransfer, RequestSource, RequestWithSources, TransferDestination
from rucio.core.rse import RseData
from rucio.db.sqla.constants import RequestType, RSEType
from rucio.transfertool.rta import RTATransfertool, lane_for
from rucio.transfertool.rta_bus import RTAConfig, job_message_id, validate_cancel, validate_job
from rucio.transfertool.transfertool import TransferToolBuilder

_SCHEMA = Path(__file__).resolve().parents[3] / "lib" / "rucio" / "transfertool" / "rta_schemas" / "v1"
_REQUEST_ID = "b4d2e8f0a1c2436590ab12cd34ef5678"
_SRC_URL = "davs://se1.example:8443/atlas/path/file.1"
_SRC_URL_2 = "davs://se1b.example/atlas/path/file.1?copy_mode=push"
_DST_URL = "davs://se2.example/atlas/path/file.1"


def _rse(
    name: str,
    rse_id: str,
    *,
    rta: bool = True,
    oidc: bool = True,
    tape: bool = False,
    staging: bool = False,
) -> RseData:
    """An RSE with the attributes submission eligibility reads."""
    attributes: dict[str, Any] = {}
    if rta:
        attributes[RseAttr.RTA_ENABLED] = True
    if oidc:
        attributes[RseAttr.OIDC_SUPPORT] = True
    if staging:
        attributes[RseAttr.STAGING_REQUIRED] = True
    info = {"rse_type": RSEType.TAPE if tape else RSEType.DISK}
    return RseData(id_=rse_id, name=name, attributes=attributes, info=info)  # type: ignore[arg-type]


def _hop(
    *,
    src: Optional[RseData] = None,
    dst: Optional[RseData] = None,
    src_scheme: str = "davs",
    dst_scheme: str = "davs",
    src_url: str = _SRC_URL,
    dst_url: str = _DST_URL,
    extra_sources: Optional[list[tuple[RseData, str, str]]] = None,
    activity: str = "Production Input",
    priority: int = 3,
    retry_count: int = 2,
    request_id: str = _REQUEST_ID,
    adler32: str = "AbC",
    md5: str = "A" * 32,
    strict_copy: bool = False,
    vo: str = "def",
) -> DirectTransfer:
    """One hop whose source_url and dest_url are the given strings, unchanged."""
    src = src or _rse("SRC_DISK", "11111111111141118111111111111111")
    dst = dst or _rse("DST_DISK", "22222222222242228222222222222222")
    account = InternalAccount("root")
    rws = RequestWithSources(
        id_=request_id,
        request_type=RequestType.TRANSFER,
        rule_id=None,
        scope=InternalScope("mc23", vo=vo),
        name="EVNT.123._000001.pool.root.1",
        md5=md5,
        adler32=adler32,
        byte_count=1234567890,
        activity=activity,
        attributes={"strict_copy": True} if strict_copy else {},
        previous_attempt_id=None,
        dest_rse=dst,
        account=account,
        retry_count=retry_count,
        priority=priority,
        transfertool="rta",
    )
    sources = [RequestSource(rse=src, scheme=src_scheme, url=src_url)]
    urls = {sources[0]: src_url}
    for extra_rse, extra_scheme, extra_url in extra_sources or []:
        extra = RequestSource(rse=extra_rse, scheme=extra_scheme, url=extra_url)
        sources.append(extra)
        urls[extra] = extra_url
    destination = TransferDestination(rse=dst, scheme=dst_scheme)

    class _Direct(DirectTransfer):
        @property
        def src(self) -> RequestSource:
            return self.sources[0]

        @property
        def dst(self) -> TransferDestination:
            return destination

        @property
        def dest_url(self) -> str:
            return dst_url

        def source_url(self, source: RequestSource) -> str:
            return urls[source]

        def dest_protocol(self):  # type: ignore[no-untyped-def]
            raise NotImplementedError

        def source_protocol(self, source: RequestSource):  # type: ignore[no-untyped-def]
            raise NotImplementedError

    return _Direct(sources, rws)


def _config(lane_by_activity: Optional[str] = None) -> RTAConfig:
    """In-memory [rta] settings. Nothing connects to NATS."""
    return RTAConfig(
        nats_servers=["nats://127.0.0.1:4222"],
        cluster_name="prod",
        lane_by_activity=lane_by_activity,
    )


def _patch_tokens(monkeypatch: pytest.MonkeyPatch, *, fail_rse: Optional[str] = None) -> list[tuple[str, list[str], list[str]]]:
    """Replace audience, scope, and minting. Record the scopes that were requested."""
    requested: list[tuple[str, list[str], list[str]]] = []

    def audience(rse_id: str) -> str:
        return "aud-" + rse_id[:8]

    def scope(rse_id: str, scopes: list[str], extra_scopes: Optional[list[str]] = None) -> str:
        requested.append((rse_id, list(scopes), list(extra_scopes or [])))
        return "storage.read"

    def mint(audience: str, scope: str, use_cache: bool = True) -> Optional[str]:
        if fail_rse is not None and audience.endswith(fail_rse[:8]):
            return None
        return "tok-" + audience

    monkeypatch.setattr("rucio.transfertool.rta.determine_audience_for_rse", audience)
    monkeypatch.setattr("rucio.transfertool.rta.determine_scope_for_rse", scope)
    monkeypatch.setattr("rucio.transfertool.rta.request_token", mint)
    return requested


@pytest.mark.parametrize(
    ("priority", "activity", "lane_by_activity", "expected"),
    [
        (5, "Production Input", None, "high"),
        (4, "Production Input", None, "high"),
        (3, "Production Input", None, "normal"),
        (2, "Production Input", None, "low"),
        (1, "Production Input", None, "low"),
        (0, "Production Input", None, "low"),
        (6, "Production Input", None, "high"),
        (None, "Production Input", None, "normal"),
        (1, "Express", '{"Express": "high"}', "high"),
        (5, "User Subscriptions", '{"User Subscriptions": "low"}', "low"),
        (5, "Other", '{"Express": "low"}', "high"),
        (1, "Other", '{"Express": "high"}', "low"),
        (3, "Express", '{"Express": "urgent"}', "normal"),
        (1, "Express", "not-json", "low"),
    ],
)
def test_lane_for_priority_and_activity(
    priority: Optional[int],
    activity: str,
    lane_by_activity: Optional[str],
    expected: str,
) -> None:
    """Priority bands are 4-5 high, 3 normal, and 0-2 low. Activity JSON overrides them."""
    assert lane_for(priority, activity, lane_by_activity, logger=logging.log) == expected


@pytest.mark.parametrize(
    "mutate",
    [
        "no_rta",
        "no_oidc",
        "https",
        "tape_dest",
        "tape_source",
        "staging_source",
        "one_source_not_davs",
        "no_rta_dest",
        "no_oidc_dest",
        "https_dest",
    ],
)
def test_ineligible_path_returns_empty(monkeypatch: pytest.MonkeyPatch, mutate: str) -> None:
    """A hop that is not davs, token-capable, and disk returns [] so fts3 can take it."""
    monkeypatch.setattr("rucio.transfertool.rta.RTAConfig.from_config", lambda: _config())
    src = _rse(
        "SRC_DISK",
        "11111111111141118111111111111111",
        rta=mutate != "no_rta",
        oidc=mutate != "no_oidc",
        tape=mutate == "tape_source",
        staging=mutate == "staging_source",
    )
    dst = _rse(
        "DST_DISK",
        "22222222222242228222222222222222",
        rta=mutate != "no_rta_dest",
        oidc=mutate != "no_oidc_dest",
        tape=mutate == "tape_dest",
    )
    extra = None
    src_scheme = "https" if mutate == "https" else "davs"
    dst_scheme = "https" if mutate == "https_dest" else "davs"
    if mutate == "one_source_not_davs":
        extra = [(_rse("SRC_OTHER", "33333333333343338333333333333333"), "https", "https://other.example/file.1")]
    hop = _hop(src=src, dst=dst, src_scheme=src_scheme, dst_scheme=dst_scheme, extra_sources=extra)
    assigned, builder = RTATransfertool.submission_builder_for_path([hop])
    assert assigned == []
    assert builder is None


def test_single_hop_builds_rta_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """An eligible davs hop is submitted to rta://<cluster>."""
    monkeypatch.setattr("rucio.transfertool.rta.RTAConfig.from_config", lambda: _config())
    hop = _hop()
    assigned, builder = RTATransfertool.submission_builder_for_path([hop])
    assert assigned == [hop]
    assert isinstance(builder, TransferToolBuilder)
    assert dict(builder.fixed_kwargs)["external_host"] == "rta://prod"
    assert RTATransfertool.can_perform_transfer(hop.src.rse, hop.dst.rse)
    assert not RTATransfertool.can_perform_transfer(
        _rse("TAPE", "44444444444444448444444444444444", tape=True),
        hop.dst.rse,
    )


def test_multihop_submits_only_the_first_eligible_hop(monkeypatch: pytest.MonkeyPatch) -> None:
    """A multi-hop path contributes only its first hop. A later hop is not submitted with it."""
    monkeypatch.setattr("rucio.transfertool.rta.RTAConfig.from_config", lambda: _config())
    first = _hop()
    second = _hop(
        src=_rse("MID_DISK", "33333333333343338333333333333333"),
        dst=_rse("TAPE", "44444444444444448444444444444444", tape=True),
        request_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    assigned, builder = RTATransfertool.submission_builder_for_path([first, second])
    assert assigned == [first]
    assert builder is not None

    blocked = _hop(
        src=_rse("TAPE", "44444444444444448444444444444444", tape=True),
        dst=_rse("DST_DISK", "22222222222242228222222222222222"),
    )
    later = _hop()
    assigned, builder = RTATransfertool.submission_builder_for_path([blocked, later])
    assert assigned == []
    assert builder is None


def test_group_into_submit_jobs_is_one_file_per_job() -> None:
    """Each file is its own job. Two paths do not share a submission."""
    tool = RTATransfertool(external_host="rta://prod", bus=MagicMock())
    first = _hop()
    second = _hop(request_id="cccccccccccccccccccccccccccccccc")
    jobs = tool.group_into_submit_jobs([[first], [second]])
    assert len(jobs) == 2
    assert jobs[0]["transfers"] == [first]
    assert jobs[1]["transfers"] == [second]


def test_submit_job_validates_and_keeps_pfns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The published TransferJob matches schema v1 and copies the submitted PFNs byte for byte."""
    scopes = _patch_tokens(monkeypatch)
    monkeypatch.setattr("rucio.transfertool.rta._scitags_ids", lambda logger=logging.log: (6, {"Production Input": 3}))
    bus = MagicMock()
    tool = RTATransfertool(external_host="rta://prod", bus=bus, lane_by_activity=None)
    extra = [(_rse("SRC_B", "55555555555545558555555555555555"), "davs", _SRC_URL_2)]
    hop = _hop(extra_sources=extra, priority=3, retry_count=2)
    transfer_id = tool.submit([hop], {})

    assert transfer_id == _REQUEST_ID
    bus.publish_job.assert_called_once()
    subject, payload, msg_id = bus.publish_job.call_args.args[:3]
    assert subject == "rta.v1.jobs.normal.def.DST_DISK"
    assert msg_id == job_message_id(_REQUEST_ID, 2)
    assert msg_id == f"{_REQUEST_ID}:2"
    document = validate_job(payload)
    assert document["sources"][0]["url"] == _SRC_URL
    assert document["sources"][1]["url"] == _SRC_URL_2
    assert document["destination"]["url"] == _DST_URL
    checksums = document["file"]["checksums"]
    assert checksums["adler32"] == "00000abc"
    assert checksums["md5"] == "a" * 32
    assert document["options"]["checksum_algo"] == "adler32"
    assert document["options"]["checksum_mode"] == "both"
    assert document["options"]["scitag"] == (6 << 6 | 3)
    assert document["lane"] == "normal"
    assert document["attempt"] == 2
    source_scopes = [call for call in scopes if call[0].startswith("1111")]
    dest_scopes = [call for call in scopes if call[0].startswith("2222")]
    assert source_scopes[0][1] == ["storage.read"]
    assert source_scopes[0][2] == ["offline_access"]
    assert dest_scopes[0][1] == ["storage.modify", "storage.read"]
    assert dest_scopes[0][2] == ["offline_access"]


def test_activity_lane_is_on_the_subject(monkeypatch: pytest.MonkeyPatch) -> None:
    """lane_by_activity overrides a low priority when the job is published."""
    _patch_tokens(monkeypatch)
    bus = MagicMock()
    tool = RTATransfertool(
        external_host="rta://prod",
        bus=bus,
        lane_by_activity='{"Express": "high"}',
    )
    hop = _hop(activity="Express", priority=1)
    tool.submit([hop], {})
    subject = bus.publish_job.call_args.args[0]
    assert subject == "rta.v1.jobs.high.def.DST_DISK"


def test_unsafe_subject_falls_back_and_does_not_mint(monkeypatch: pytest.MonkeyPatch) -> None:
    """A dotted VO or RSE name is not an RTA hop, and submit does not mint tokens for it."""
    monkeypatch.setattr("rucio.transfertool.rta.RTAConfig.from_config", lambda: _config())
    for hop in (
        _hop(dst=_rse("DST.DISK", "22222222222242228222222222222222")),
        _hop(src=_rse("SRC.DISK", "11111111111141118111111111111111")),
        _hop(vo="atlas.prod"),
    ):
        assigned, builder = RTATransfertool.submission_builder_for_path([hop])
        assert assigned == []
        assert builder is None

    scopes = _patch_tokens(monkeypatch)
    bus = MagicMock()
    tool = RTATransfertool(external_host="rta://prod", bus=bus)
    with pytest.raises(TransferToolWrongAnswer):
        tool.submit([_hop(dst=_rse("DST.DISK", "22222222222242228222222222222222"))], {})
    bus.publish_job.assert_not_called()
    assert scopes == []


def test_token_mint_failure_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing token is raised and nothing is published."""
    _patch_tokens(monkeypatch, fail_rse="22222222222242228222222222222222")
    bus = MagicMock()
    tool = RTATransfertool(external_host="rta://prod", bus=bus)
    with pytest.raises(RucioException, match="DST_DISK"):
        tool.submit([_hop()], {})
    bus.publish_job.assert_not_called()


def test_bulk_query_reads_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stored non-terminal value is an empty report, which the poller leaves submitted."""
    bus = MagicMock()

    def read(transfer_id: str, timeout: Optional[float] = None) -> Optional[bytes]:
        if transfer_id == "missing":
            return None
        return b'{"agent_id":"rta-1","lane":"normal","state":"active","attempt":1,"updated_at":"2026-11-27T09:00:00Z"}'

    bus.kv_get_state.side_effect = read
    tool = RTATransfertool(external_host="rta://prod", bus=bus)
    result = tool.bulk_query({"missing": {"req": {}}, "present": {"req": {}}})
    assert result["missing"] == {}
    assert result["present"] == {}


def test_missing_rta_state_stays_submitted() -> None:
    """A missing rta_state key is not LOST. The poller treats {} as still SUBMITTED."""
    bus = MagicMock()
    bus.kv_get_state.return_value = None
    tool = RTATransfertool(external_host="rta://prod", bus=bus)
    result = tool.bulk_query({"queued": {"req-1": {"state": "S"}}})
    assert result["queued"] == {}
    assert result["queued"] is not None


def test_cancel_puts_a_valid_tombstone() -> None:
    """cancel writes a schema-valid Cancel into rta_cancel."""
    bus = MagicMock()
    bus.put_cancel.return_value = 3
    tool = RTATransfertool(external_host="rta://prod", bus=bus)
    tool.cancel([_REQUEST_ID])
    transfer_id, payload = bus.put_cancel.call_args.args[:2]
    assert transfer_id == _REQUEST_ID
    document = validate_cancel(payload)
    assert document["transfer_id"] == _REQUEST_ID
    assert document["request_id"] == _REQUEST_ID
    assert document["v"] == 1


def test_update_priority_only_logs(caplog: pytest.LogCaptureFixture) -> None:
    """A queued job is not moved between lanes."""
    bus = MagicMock()
    tool = RTATransfertool(external_host="rta://prod", bus=bus, logger=logging.getLogger("rta-test").log)
    with caplog.at_level(logging.INFO, logger="rta-test"):
        tool.update_priority(_REQUEST_ID, 5)
    bus.publish_job.assert_not_called()
    bus.put_cancel.assert_not_called()
    bus.kv_get_state.assert_not_called()
    assert "lane" in caplog.text
    assert _REQUEST_ID in caplog.text


def test_schema_says_msg_id_includes_attempt() -> None:
    """The vendored job description matches Nats-Msg-Id = transfer_id:attempt."""
    job = json.loads((_SCHEMA / "transfer_job.json").read_text(encoding="utf-8"))
    result = json.loads((_SCHEMA / "transfer_result.json").read_text(encoding="utf-8"))
    assert "transfer_id:attempt" in job["description"]
    assert "Nats-Msg-Id equal to transfer_id." not in job["description"]
    assert "transfer_id:attempt" in job["properties"]["transfer_id"]["description"]
    message = result["$defs"]["transferError"]["properties"]["message"]["description"]
    assert "Destination file exists and overwrite is not enabled" in message


def test_rta_is_registered() -> None:
    """The class is reachable by external name and the RSE attribute exists."""
    from rucio.core.transfer import TRANSFERTOOL_CLASSES_BY_NAME

    assert RseAttr.RTA_ENABLED == "rta_enabled"
    assert TRANSFERTOOL_CLASSES_BY_NAME["rta"] is RTATransfertool
    assert RTATransfertool.external_name == "rta"
