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

"""RTA transfer tool.

Submits one WebDAV third-party copy per file. Eligibility is a single davs
hop between two disk RSEs that both have ``rta_enabled`` and ``oidc_support``.
Anything else returns an empty path so the conveyor can hand the transfer to
fts3.

The published TransferJob is validated by :func:`rucio.transfertool.rta_bus.validate_job`.
Source and destination URLs are the PFNs ``DirectTransfer`` already generated:
the finisher matches ``source_url_used`` with ``==``, so those strings are not
rewritten. Checksums are lowercase hex, adler32 padded to 8 characters and md5
to 32. ``Nats-Msg-Id`` is ``<transfer_id>:<attempt>``.

The exact EEXIST sentence (``Destination file exists and overwrite is not
enabled``) lives in the vendored result schema. This module does not reword it.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import threading
from typing import TYPE_CHECKING, Any

from rucio.common.constants import RseAttr
from rucio.common.exception import RucioException, TransferToolTimeout, TransferToolWrongAnswer
from rucio.core.oidc import request_token
from rucio.core.rse import determine_audience_for_rse, determine_scope_for_rse
from rucio.transfertool.fts3 import _pick_fts_checksum, _scitags_ids, build_job_params
from rucio.transfertool.rta_bus import (
    RTABus,
    RTABusError,
    RTAConfig,
    RTASchemaError,
    RTATimeoutError,
    job_message_id,
    job_subject,
)
from rucio.transfertool.transfertool import Transfertool, TransferToolBuilder

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from rucio.common.types import LoggerFunction
    from rucio.core.request import DirectTransfer
    from rucio.core.rse import RseData

_CHECKSUM_WIDTH = {
    "adler32": 8,
    "crc32": 8,
    "md5": 32,
    "sha256": 64,
}
_HEX = re.compile(r"^[0-9a-f]+$")
_LANES = ("high", "normal", "low")
_CHECKSUM_MODES = ("none", "source", "target", "both")
_DEFAULT_EXPIRES = datetime.timedelta(hours=12)
_CANCEL_REASON = "request cancelled"

_BUS_LOCK = threading.Lock()
_BUS: RTABus | None = None


def lane_for(
    priority: int | None,
    activity: str,
    lane_by_activity: str | None = None,
    logger: "LoggerFunction" = logging.log,
) -> str:
    """
    Choose ``high``, ``normal``, or ``low``.

    The priority bands match the Rucio scale used for FTS: 4 and above are
    ``high``, 3 is ``normal``, and 2 and below are ``low``. A missing priority
    is 3. ``lane_by_activity`` is a JSON object of activity name to lane and
    overrides the band. An unusable map is ignored.

    :param priority: Request priority. Higher is more urgent.
    :param activity: Request activity, for example ``Production Input``.
    :param lane_by_activity: ``[rta] lane_by_activity`` value, or None.
    :param logger: Logger used when the map cannot be read.
    :returns: The lane placed in the job subject.
    """
    override = _activity_lane(activity, lane_by_activity, logger)
    if override is not None:
        return override
    return _lane_for_priority(priority)


def _lane_for_priority(priority: int | None) -> str:
    """Map a Rucio priority onto a lane. Missing and non-integers use normal."""
    if isinstance(priority, bool) or not isinstance(priority, int):
        return "normal"
    if priority >= 4:
        return "high"
    if priority <= 2:
        return "low"
    return "normal"


def _activity_lane(activity: str, raw: str | None, logger: "LoggerFunction") -> str | None:
    """Return the configured lane for ``activity``, or None when it does not apply."""
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger(logging.WARNING, "RTA lane_by_activity is not JSON and will be ignored")
        return None
    if not isinstance(parsed, dict):
        logger(logging.WARNING, "RTA lane_by_activity must be a JSON object")
        return None
    lane = parsed.get(activity)
    if lane in _LANES:
        return str(lane)
    if lane is not None:
        logger(logging.WARNING, "RTA lane_by_activity has an unknown lane for %s", activity)
    return None


def _normalize_checksum(name: str, value: str) -> str | None:
    """
    Return lowercase hex of the schema width, or None.

    Shorter hex is zero-padded on the left so an adler32 of ``abc`` becomes
    ``00000abc``. Values that are not hex, or that are already wider than the
    schema, are dropped.
    """
    width = _CHECKSUM_WIDTH.get(name)
    if width is None:
        return None
    text = value.strip().lower()
    if _HEX.fullmatch(text) is None or len(text) > width:
        return None
    return text.zfill(width)


def _stamp(moment: datetime.datetime) -> str:
    """Format an instant as RFC 3339 UTC with a ``Z`` suffix and no fraction."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _job_priority(priority: int | None) -> int:
    """Return a non-negative integer priority for the job document."""
    if isinstance(priority, bool) or not isinstance(priority, int):
        return 3
    if priority < 0:
        return 0
    return priority


def get_rta_bus() -> RTABus:
    """
    Return the process-wide bus, connecting on first use.

    :returns: A started :class:`RTABus`.
    :raises RTABusError: ``[rta]`` configuration is missing or invalid.
    :raises RTATimeoutError: The connection did not complete in time.
    """
    global _BUS
    with _BUS_LOCK:
        if _BUS is None:
            config = RTAConfig.from_config()
            bus = RTABus(config)
            bus.start()
            _BUS = bus
        return _BUS


class RTATransfertool(Transfertool):
    """
    Submit HTTPS/WebDAV third-party copies to RTA over NATS.

    ``external_host`` is ``rta://<cluster_name>``. One submit call publishes
    one TransferJob and returns its ``transfer_id``, which Rucio stores as
    ``external_id``. That id is the request id, so a repeated submit of the
    same attempt is deduplicated by ``Nats-Msg-Id``.
    """

    external_name = "rta"
    supported_schemes = {"davs"}
    required_rse_attrs = (RseAttr.RTA_ENABLED, RseAttr.OIDC_SUPPORT)

    def __init__(
        self,
        external_host: str,
        logger: "LoggerFunction" = logging.log,
        lane_by_activity: str | None = None,
        bus: RTABus | None = None,
    ) -> None:
        """
        :param external_host: ``rta://<cluster_name>``.
        :param logger: Logger passed through from the conveyor.
        :param lane_by_activity: JSON activity map from ``[rta]``. None uses priority only.
        :param bus: Open bus. None connects via :func:`get_rta_bus` on first use.
        """
        super().__init__(external_host, logger)
        self._lane_by_activity = lane_by_activity
        self._bus = bus

    @classmethod
    def can_perform_transfer(cls, source_rse: "RseData", dest_rse: "RseData") -> bool:
        """True when both RSEs are non-tape and have RTA and OIDC enabled."""
        return cls._rse_eligible(source_rse) and cls._rse_eligible(dest_rse)

    @classmethod
    def _rse_eligible(cls, rse: "RseData") -> bool:
        """True when the RSE is a non-tape RSE with RTA and OIDC enabled."""
        try:
            attributes = rse.attributes
            tape = rse.is_tape()
        except ValueError:
            return False
        if tape:
            return False
        if attributes.get(RseAttr.RTA_ENABLED) is not True:
            return False
        if attributes.get(RseAttr.OIDC_SUPPORT) is not True:
            return False
        return True

    @classmethod
    def _hop_eligible(cls, hop: "DirectTransfer") -> bool:
        """True when every endpoint of the hop is davs and :meth:`_rse_eligible`."""
        if not hop.sources:
            return False
        for source in hop.sources:
            if source.scheme != "davs" or not cls._rse_eligible(source.rse):
                return False
        if hop.dst.scheme != "davs" or not cls._rse_eligible(hop.dst.rse):
            return False
        return True

    @classmethod
    def submission_builder_for_path(
        cls,
        transfer_path: "list[DirectTransfer]",
        logger: "LoggerFunction" = logging.log,
    ) -> "tuple[list[DirectTransfer], TransferToolBuilder | None]":
        """
        Accept the first hop when it is an eligible davs disk-to-disk copy.

        Later hops are left for the conveyor. An ineligible first hop returns
        ``([], None)`` so the next configured transfertool, usually fts3, can
        take the path.

        :param transfer_path: Candidate hops, source to destination.
        :param logger: Logger passed through from the conveyor.
        :returns: The first hop and a builder, or an empty path.
        """
        if not transfer_path:
            return [], None
        hop = transfer_path[0]
        if not cls._hop_eligible(hop):
            logger(logging.INFO, "RTATransfertool cannot submit %s", hop)
            return [], None
        try:
            config = RTAConfig.from_config()
        except RTABusError:
            logger(logging.ERROR, "RTATransfertool has no usable [rta] configuration", exc_info=True)
            return [], None
        if len(transfer_path) > 1:
            logger(
                logging.INFO,
                "RTATransfertool submits only the first hop of %s",
                [str(item) for item in transfer_path],
            )
        return [hop], TransferToolBuilder(
            cls,
            external_host="rta://%s" % config.cluster_name,
            lane_by_activity=config.lane_by_activity,
        )

    def group_into_submit_jobs(self, transfer_paths: "Iterable[list[DirectTransfer]]") -> list[dict[str, Any]]:
        """
        Return one job per file.

        :param transfer_paths: Paths already reduced to the hop this tool submits.
        :returns: Jobs of the form ``{"transfers": [hop], "job_params": {}}``.
        """
        jobs = []
        for path in transfer_paths:
            if not path:
                continue
            jobs.append({"transfers": [path[0]], "job_params": {}})
        return jobs

    def _bus_or_default(self) -> RTABus:
        """Return the injected bus, or the process-wide one."""
        if self._bus is not None:
            return self._bus
        return get_rta_bus()

    def _mint(self, rse_id: str, rse_name: str, scopes: list[str]) -> str:
        """
        Mint one bearer token. The scopes match FTS, including ``storage.read``
        on the destination so a StoRM stat succeeds.

        :param rse_id: RSE id passed to the audience and scope helpers.
        :param rse_name: Used in the error when minting returns nothing.
        :param scopes: Storage scopes, without ``offline_access``.
        :returns: The access token. It is not logged.
        :raises RucioException: The token endpoint returned nothing.
        """
        audience = determine_audience_for_rse(rse_id)
        scope = determine_scope_for_rse(rse_id, scopes=scopes, extra_scopes=["offline_access"])
        token = request_token(audience, scope)
        if not token:
            raise RucioException("Failed to mint an RTA token for RSE %s" % rse_name)
        return token

    def _checksum_options(self, transfer: "DirectTransfer", mode: str) -> tuple[str, str, dict[str, str]]:
        """
        Reuse the FTS checksum strategy and the picked algorithm.

        :param transfer: The single hop being submitted.
        :param mode: ``verify_checksum`` from :func:`build_job_params`.
        :returns: ``checksum_mode``, ``checksum_algo``, and the normalized map.
        """
        if mode not in _CHECKSUM_MODES:
            mode = "none"
        picked = _pick_fts_checksum(transfer, path_strategy=mode) if mode != "none" else None
        if not picked or ":" not in picked:
            return "none", "adler32", {}
        algo = picked.split(":", 1)[0].lower()
        checksums: dict[str, str] = {}
        for name in _CHECKSUM_WIDTH:
            raw = getattr(transfer.rws, name, None)
            if not isinstance(raw, str) or not raw:
                continue
            normalized = _normalize_checksum(name, raw)
            if normalized is not None:
                checksums[name] = normalized
        if algo not in checksums:
            return "none", "adler32", {}
        return mode, algo, checksums

    def _scitag(self, activity: str) -> int:
        """Return the SciTag flow label, or 0 when packet marking has no ids."""
        exp_id, activity_ids = _scitags_ids(logger=self.logger)
        if not isinstance(exp_id, int):
            return 0
        activity_id = activity_ids.get(activity)
        if not isinstance(activity_id, int):
            return 0
        value = (exp_id << 6) | activity_id
        if value < 0 or value > 65535:
            return 0
        return value

    def _build_job(self, transfer: "DirectTransfer") -> tuple[dict[str, Any], str, str]:
        """
        Build one TransferJob, its subject, and its ``Nats-Msg-Id``.

        URLs are the values ``source_url`` and ``dest_url`` return. They are
        not parsed or rewritten.

        :param transfer: The file to submit.
        :returns: The job document, the subject, and ``transfer_id:attempt``.
        :raises RucioException: The hop has no request id or no sources, or a token is missing.
        """
        request_id = transfer.rws.request_id
        if not isinstance(request_id, str) or not request_id:
            raise RucioException("RTA submit requires a request id")
        if not transfer.sources:
            raise RucioException("No sources defined")

        attempt = transfer.rws.retry_count
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
            attempt = 0
        priority = _job_priority(transfer.rws.priority)
        activity = str(transfer.rws.activity)
        lane = lane_for(transfer.rws.priority, activity, self._lane_by_activity, logger=self.logger)
        scope = transfer.rws.scope
        vo = scope.vo if scope is not None and scope.vo else "def"
        scope_name = scope.external if scope is not None and scope.external else str(scope)
        created = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        lifetime = transfer.rws.attributes.get("lifetime")
        if isinstance(lifetime, bool) or not isinstance(lifetime, int) or lifetime <= 0:
            expires = created + _DEFAULT_EXPIRES
        else:
            expires = created + datetime.timedelta(seconds=lifetime)

        params = build_job_params([transfer], logger=self.logger)
        mode, algo, checksums = self._checksum_options(transfer, str(params.get("verify_checksum") or "none"))
        sources = []
        for source in transfer.sources:
            sources.append(
                {
                    "url": transfer.source_url(source),
                    "rse": source.rse.name,
                    "rse_id": source.rse.id,
                    "token": self._mint(source.rse.id, source.rse.name, ["storage.read"]),
                }
            )
        destination = {
            "url": transfer.dest_url,
            "rse": transfer.dst.rse.name,
            "rse_id": transfer.dst.rse.id,
            "token": self._mint(transfer.dst.rse.id, transfer.dst.rse.name, ["storage.modify", "storage.read"]),
        }
        job = {
            "v": 1,
            "transfer_id": request_id,
            "request_id": request_id,
            "attempt": attempt,
            "vo": vo,
            "activity": activity,
            "priority": priority,
            "lane": lane,
            "created_at": _stamp(created),
            "expires_at": _stamp(expires),
            "sources": sources,
            "destination": destination,
            "file": {
                "scope": scope_name,
                "name": transfer.rws.name,
                "size": int(transfer.rws.byte_count or 0),
                "checksums": checksums,
            },
            "options": {
                "checksum_mode": mode,
                "checksum_algo": algo,
                "overwrite": bool(params.get("overwrite", True)),
                "strict_copy": bool(params.get("strict_copy", False)),
                "create_parent": True,
                "cleanup_on_failure": True,
                "copy_mode": "pull",
                "fallback_push": False,
                "streams": 1,
                "timeout_s": 0,
                "scitag": self._scitag(activity),
                "ip_preference": "v4" if params.get("ipv4") else "any",
            },
            "metadata": {},
        }
        subject = job_subject(lane, vo, transfer.dst.rse.name)
        return job, subject, job_message_id(request_id, attempt)

    def submit(
        self,
        transfers: "Iterable[DirectTransfer]",
        job_params: dict[str, str],
        timeout: int | None = None,
    ) -> str:
        """
        Publish one TransferJob and return its transfer id.

        :param transfers: Exactly one hop. Grouping guarantees that.
        :param job_params: Unused. Checksum, overwrite, and scitag are computed here.
        :param timeout: Seconds to wait for the PubAck.
        :returns: ``transfer_id``, stored by Rucio as ``external_id``.
        :raises RucioException: The job is not one file, or a token could not be minted.
        :raises TransferToolTimeout: The PubAck did not arrive in time.
        :raises TransferToolWrongAnswer: The bus rejected the job.
        """
        del job_params  # parameters are derived per file, not from the FTS bulk group
        files = list(transfers)
        if len(files) != 1:
            raise RucioException("RTATransfertool submits one file per job")
        job, subject, msg_id = self._build_job(files[0])
        payload = json.dumps(job).encode("utf-8")
        limit = None if timeout is None else float(timeout)
        self.logger(logging.INFO, "Publishing RTA job %s on %s", job["transfer_id"], subject)
        try:
            ack = self._bus_or_default().publish_job(subject, payload, msg_id, timeout=limit)
        except RTATimeoutError as error:
            raise TransferToolTimeout(error) from error
        except (RTASchemaError, RTABusError) as error:
            raise TransferToolWrongAnswer(error) from error
        if ack.duplicate:
            self.logger(logging.INFO, "RTA job %s attempt was already published", job["transfer_id"])
        return str(job["transfer_id"])

    def bulk_query(
        self,
        requests_by_eid: "Mapping[str, Mapping[str, Any]]",
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Read ``rta_state`` for each transfer id.

        A missing key is lost (``None``), which the poller times out. A stored
        value means the job is still known, so the request stays ``SUBMITTED``.
        Terminal ``DONE`` / ``FAILED`` / ``CANCELED`` updates arrive on the
        results stream, not through this poll.

        :param requests_by_eid: ``{external_id: {request_id: request}}``.
        :param timeout: Seconds to wait for each KV read.
        :returns: ``None`` when the key is absent, ``{}`` when it is present,
            or the exception when the read fails.
        """
        limit = None if timeout is None else float(timeout)
        bus = self._bus_or_default()
        responses: dict[str, Any] = {}
        for transfer_id in requests_by_eid:
            try:
                payload = bus.kv_get_state(transfer_id, timeout=limit)
            except RTABusError as error:
                responses[transfer_id] = error
                continue
            if payload is None:
                responses[transfer_id] = None
                continue
            try:
                document = json.loads(payload)
            except json.JSONDecodeError as error:
                responses[transfer_id] = error
                continue
            if not isinstance(document, dict):
                responses[transfer_id] = RTABusError("rta_state value is not an object")
                continue
            responses[transfer_id] = {}
        return responses

    def cancel(self, transfer_ids: "Sequence[str]", timeout: int | None = None) -> None:
        """
        Write one Cancel tombstone per id into ``rta_cancel``.

        ``transfer_id`` is the request id, so both schema fields carry it.

        :param transfer_ids: External ids returned by :meth:`submit`.
        :param timeout: Seconds to wait for each KV write.
        :raises TransferToolTimeout: A write did not finish in time.
        :raises TransferToolWrongAnswer: The bus rejected the tombstone.
        """
        limit = None if timeout is None else float(timeout)
        bus = self._bus_or_default()
        issued_at = _stamp(datetime.datetime.now(datetime.timezone.utc))
        for transfer_id in transfer_ids:
            document = {
                "v": 1,
                "transfer_id": transfer_id,
                "request_id": transfer_id,
                "reason": _CANCEL_REASON,
                "issued_at": issued_at,
            }
            payload = json.dumps(document).encode("utf-8")
            self.logger(logging.INFO, "Cancelling RTA transfer %s", transfer_id)
            try:
                bus.put_cancel(transfer_id, payload, timeout=limit)
            except RTATimeoutError as error:
                raise TransferToolTimeout(error) from error
            except (RTASchemaError, RTABusError) as error:
                raise TransferToolWrongAnswer(error) from error

    def update_priority(self, transfer_id: str, priority: int, timeout: int | None = None) -> None:
        """
        Log that a queued job keeps the lane it was submitted on.

        A later submission uses the new priority. Moving a job that is already
        on a lane would mean cancelling and resubmitting, which is out of scope.

        :param transfer_id: External id of the queued job.
        :param priority: The priority Rucio would use for a new submission.
        :param timeout: Unused. The bus is not called.
        """
        del timeout
        self.logger(
            logging.INFO,
            "RTA transfer %s stays in its queued lane; priority %s applies to jobs submitted from now on",
            transfer_id,
            priority,
        )
