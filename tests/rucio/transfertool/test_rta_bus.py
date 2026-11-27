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

"""Unit tests for the RTA NATS bus.

The live tests start the official ``nats`` image with ``-js``. They skip when
Docker or that image is not available. Schema and config tests do not need a
server.
"""

import asyncio
import configparser
import json
import shutil
import socket
import subprocess  # noqa: S404 -- docker starts the nats-server container
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from rucio.transfertool.rta_bus import (
    CANCEL_BUCKET,
    JOBS_STREAM,
    RESULTS_STREAM,
    RTA_CONFIG_KEYS,
    STATE_BUCKET,
    RTABus,
    RTABusError,
    RTAConfig,
    RTASchemaError,
    job_subject,
    results_consumer,
    results_subject,
    token_subject,
    validate_job,
    validate_result,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

try:
    import nats
    from nats.js.api import StorageType
except ImportError:
    nats = None
    StorageType = None


def _token(prefix: str) -> str:
    """Return a NATS-safe id."""
    return prefix + uuid.uuid4().hex


def _job(transfer_id: str) -> bytes:
    """A TransferJob that matches the vendored v1 schema."""
    document = {
        "v": 1,
        "transfer_id": transfer_id,
        "request_id": "b4d2e8f0a1c2436590ab12cd34ef5678",
        "attempt": 2,
        "vo": "atlas",
        "activity": "Production Input",
        "priority": 3,
        "lane": "normal",
        "created_at": "2026-11-27T09:00:00Z",
        "expires_at": "2026-11-27T21:00:00Z",
        "sources": [
            {
                "url": "davs://se1.example:443/atlas/mc23/EVNT.123._000001.pool.root.1",
                "rse": "SITE_A_DATADISK",
                "rse_id": "11111111-1111-4111-8111-111111111111",
                "token": "eyJhbGciOiJub25lIn0.eyJzdWIiOiJzcmMifQ.sig",
            }
        ],
        "destination": {
            "url": "davs://se2.example:443/atlas/mc23/EVNT.123._000001.pool.root.1",
            "rse": "SITE_B_DATADISK",
            "rse_id": "22222222-2222-4222-8222-222222222222",
            "token": "eyJhbGciOiJub25lIn0.eyJzdWIiOiJkc3QifQ.sig",
        },
        "file": {
            "scope": "mc23",
            "name": "EVNT.123._000001.pool.root.1",
            "size": 1234567890,
            "checksums": {"adler32": "0a1b2c3d"},
        },
        "options": {
            "checksum_mode": "both",
            "checksum_algo": "adler32",
            "overwrite": True,
            "strict_copy": False,
            "create_parent": True,
            "cleanup_on_failure": True,
            "copy_mode": "pull",
            "fallback_push": False,
            "streams": 1,
            "timeout_s": 0,
            "scitag": 2753,
            "ip_preference": "any",
        },
        "metadata": {},
    }
    return json.dumps(document).encode()


def _result(transfer_id: str) -> bytes:
    """A DONE TransferResult that matches the vendored v1 schema."""
    document = {
        "v": 1,
        "transfer_id": transfer_id,
        "request_id": "b4d2e8f0a1c2436590ab12cd34ef5678",
        "attempt": 2,
        "agent_id": "rta-7f9c-2",
        "delivery": 1,
        "state": "DONE",
        "source_url_used": "davs://se1.example:443/atlas/mc23/EVNT.123._000001.pool.root.1",
        "copy_mode_used": "pull",
        "ip_version": "ipv6",
        "submitted_at": "2026-11-27T09:00:00Z",
        "started_at": "2026-11-27T09:00:01Z",
        "finished_at": "2026-11-27T09:00:31Z",
        "bytes": 1234567890,
        "avg_throughput_bps": 412000000,
        "checksum": {"algo": "adler32", "source": "0a1b2c3d", "destination": "0a1b2c3d"},
        "error": None,
        "dst_file": None,
        "metadata": {},
    }
    return json.dumps(document).encode()


def _config(url: str) -> RTAConfig:
    """A short-timeout configuration pointed at one test server."""
    return RTAConfig(
        nats_servers=[url],
        connect_timeout=5,
        operation_timeout=5,
        reconnect_time_wait=1,
        max_reconnect_attempts=-1,
    )


def test_config_from_parser_reads_rta_keys() -> None:
    """The [rta] keys documented for operators round-trip through RTAConfig."""
    parser = configparser.ConfigParser()
    parser.read_dict({
        "rta": {
            "nats_servers": "nats://a.example:4222, tls://b.example:4222",
            "nats_creds": "/etc/rta.creds",
            "tls_ca": "/etc/ca.pem",
            "tls_cert": "/etc/cert.pem",
            "tls_key": "/etc/key.pem",
            "cluster_name": "prod",
            "connect_timeout": "3",
            "operation_timeout": "4.5",
            "reconnect_time_wait": "1",
            "max_reconnect_attempts": "-1",
            "result_batch": "10",
            "token_min_valid": "3900",
            "lane_by_priority": "1:high",
            "lane_by_activity": "User Subscriptions:low",
        }
    })
    config = RTAConfig.from_parser(parser)
    assert config.nats_servers == ["nats://a.example:4222", "tls://b.example:4222"]
    assert config.nats_creds == "/etc/rta.creds"
    assert config.tls_ca == "/etc/ca.pem"
    assert config.tls_cert == "/etc/cert.pem"
    assert config.tls_key == "/etc/key.pem"
    assert config.cluster_name == "prod"
    assert config.connect_timeout == 3
    assert config.operation_timeout == 4.5
    assert config.reconnect_time_wait == 1
    assert config.max_reconnect_attempts == -1
    assert config.result_batch == 10
    assert config.token_min_valid == 3900
    assert config.lane_by_priority == "1:high"
    assert config.lane_by_activity == "User Subscriptions:low"


def test_config_rejects_incomplete_values() -> None:
    """Missing servers and a half-specified client certificate are refused."""
    with pytest.raises(RTABusError, match="nats_servers"):
        RTAConfig(nats_servers=[])
    with pytest.raises(RTABusError, match="tls_cert"):
        RTAConfig(nats_servers=["nats://127.0.0.1:4222"], tls_cert="/etc/cert.pem")
    parser = configparser.ConfigParser()
    with pytest.raises(RTABusError, match="no \\[rta\\] section"):
        RTAConfig.from_parser(parser)


def test_template_documents_rta_keys() -> None:
    """etc/rucio.cfg.template names every [rta] key the bus reads."""
    template = Path(__file__).resolve().parents[3] / "etc" / "rucio.cfg.template"
    text = template.read_text(encoding="utf-8")
    section = text[text.index("# RTA NATS bus"):]
    for key in RTA_CONFIG_KEYS:
        assert key in section


def test_schema_source_records_the_rta_commit() -> None:
    """The vendored schemas point at the rta commit they were copied from."""
    note = Path(__file__).resolve().parents[3] / "lib" / "rucio" / "transfertool" / "rta_schemas" / "SOURCE"
    text = note.read_text(encoding="utf-8")
    assert "1ce92bf36fc817efa6436974896c859a5ff4f782" in text
    assert "cursor/rah-6-msg-schemas-fab7" in text
    assert "schema/v1/" in text


def test_schema_accepts_a_job_and_rejects_a_broken_one() -> None:
    """Outgoing jobs and incoming results are checked against the vendored schemas."""
    transfer_id = _token("job")
    validate_job(_job(transfer_id))
    validate_result(_result(transfer_id))
    broken = json.loads(_job(transfer_id))
    broken["v"] = 2
    with pytest.raises(RTASchemaError, match="transfer job"):
        validate_job(json.dumps(broken).encode())
    with pytest.raises(RTASchemaError, match="transfer result"):
        validate_result(b"{}")


def _docker() -> str:
    """Return the docker binary, or skip when a nats container cannot be started."""
    binary = shutil.which("docker")
    if binary is None:
        pytest.skip("nats-server container is absent: docker is not installed")
    info = subprocess.run([binary, "info"], capture_output=True, text=True, check=False)
    if info.returncode != 0:
        pytest.skip("nats-server container is absent: docker daemon is not available")
    return binary


def _last_line(text: str) -> str:
    """The last non-empty line of a docker error, truncated."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return "docker run failed"
    return lines[-1][:300]


async def _provision(url: str) -> None:
    """Create the v1 stream and KV names on a fresh JetStream server."""
    if nats is None:
        raise RuntimeError("nats-py is not installed")
    deadline = time.monotonic() + 20
    last_error: Exception = RuntimeError("nats did not accept a connection")
    client = None
    while time.monotonic() < deadline:
        try:
            client = await nats.connect(url, connect_timeout=1, allow_reconnect=False, max_reconnect_attempts=0)
            break
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.2)
    if client is None:
        raise last_error
    try:
        jetstream = client.jetstream()
        await jetstream.add_stream(
            name=JOBS_STREAM,
            subjects=["rta.v1.jobs.>"],
            storage=StorageType.MEMORY,
            duplicate_window=120,
        )
        await jetstream.add_stream(
            name=RESULTS_STREAM,
            subjects=["rta.v1.results.>"],
            storage=StorageType.MEMORY,
        )
        await jetstream.create_key_value(bucket=CANCEL_BUCKET, storage=StorageType.MEMORY)
        await jetstream.create_key_value(bucket=STATE_BUCKET, storage=StorageType.MEMORY)
    finally:
        await client.close()


async def _stream_messages(url: str, stream: str) -> int:
    """Return how many messages a stream currently holds."""
    client = await nats.connect(url, connect_timeout=2, allow_reconnect=False, max_reconnect_attempts=0)
    try:
        info = await client.jetstream().stream_info(stream)
        return int(info.state.messages)
    finally:
        await client.close()


async def _kv_get(url: str, bucket: str, key: str) -> bytes:
    """Read one KV value with a direct client."""
    client = await nats.connect(url, connect_timeout=2, allow_reconnect=False, max_reconnect_attempts=0)
    try:
        entry = await client.jetstream().key_value(bucket)
        got = await entry.get(key)
        if got.value is None:
            raise AssertionError(f"{bucket}/{key} has no value")
        return got.value
    finally:
        await client.close()


async def _kv_put(url: str, bucket: str, key: str, value: bytes) -> None:
    """Write one KV value with a direct client."""
    client = await nats.connect(url, connect_timeout=2, allow_reconnect=False, max_reconnect_attempts=0)
    try:
        entry = await client.jetstream().key_value(bucket)
        await entry.put(key, value)
    finally:
        await client.close()


async def _publish(url: str, subject: str, payload: bytes) -> None:
    """Publish one JetStream message with a direct client."""
    client = await nats.connect(url, connect_timeout=2, allow_reconnect=False, max_reconnect_attempts=0)
    try:
        await client.jetstream().publish(subject, payload)
    finally:
        await client.close()


async def _request(url: str, subject: str, payload: bytes) -> bytes:
    """Send one core NATS request."""
    client = await nats.connect(url, connect_timeout=2, allow_reconnect=False, max_reconnect_attempts=0)
    try:
        message = await client.request(subject, payload, timeout=3)
        return message.data
    finally:
        await client.close()


async def _ack_pending(url: str, consumer: str) -> int:
    """Return the number of outstanding acks on a results consumer."""
    client = await nats.connect(url, connect_timeout=2, allow_reconnect=False, max_reconnect_attempts=0)
    try:
        info = await client.jetstream().consumer_info(RESULTS_STREAM, consumer)
        return int(info.num_ack_pending or 0)
    finally:
        await client.close()


@pytest.fixture(scope="session")
def nats_url() -> "Iterator[str]":
    """One official nats-server with JetStream, or a skip when it cannot be started."""
    if nats is None:
        pytest.skip("nats-server container is absent: nats-py is not installed")
    binary = _docker()
    name = "rta-nats-" + uuid.uuid4().hex[:8]
    started = subprocess.run(
        [binary, "run", "-d", "--name", name, "--rm", "-p", "127.0.0.1::4222", "nats:2", "-js"],
        capture_output=True,
        text=True,
        check=False,
    )
    if started.returncode != 0:
        pytest.skip("nats-server container is absent: " + _last_line(started.stderr or started.stdout))
    try:
        mapped = subprocess.run([binary, "port", name, "4222"], capture_output=True, text=True, check=False)
        if mapped.returncode != 0:
            pytest.fail("nats-server container did not publish port 4222: " + _last_line(mapped.stderr))
        host, separator, port_text = mapped.stdout.strip().splitlines()[0].rpartition(":")
        if separator == "":
            pytest.fail("nats-server container returned an unusable port mapping")
        host = host or "127.0.0.1"
        port = int(port_text)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((host, port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            pytest.fail("nats-server container did not accept a TCP connection")
        url = f"nats://{host}:{port}"
        asyncio.run(_provision(url))
        yield url
    finally:
        subprocess.run([binary, "rm", "-f", name], capture_output=True, check=False)


@pytest.fixture
def bus(nats_url: str) -> "Iterator[RTABus]":
    """A started bus. close() runs even when the test fails."""
    client = RTABus(_config(nats_url))
    client.start()
    try:
        yield client
    finally:
        client.close()


def test_publish_dedupes_on_msg_id(bus: RTABus) -> None:
    """A second publish with the same Nats-Msg-Id is acknowledged as a duplicate."""
    transfer_id = _token("dedupe")
    payload = _job(transfer_id)
    subject = job_subject("normal", "atlas", "SITE_B_DATADISK")
    first = bus.publish_job(subject, payload, transfer_id)
    second = bus.publish_job(subject, payload, transfer_id)
    assert first.stream == JOBS_STREAM
    assert first.duplicate is False
    assert second.duplicate is True
    assert second.seq == first.seq


def test_publish_schema_rejection_does_not_store_a_message(bus: RTABus, nats_url: str) -> None:
    """An invalid job raises before JetStream stores it."""
    before = asyncio.run(_stream_messages(nats_url, JOBS_STREAM))
    transfer_id = _token("badjob")
    broken = json.loads(_job(transfer_id))
    broken["v"] = 2
    subject = job_subject("normal", "atlas", "SITE_B_DATADISK")
    with pytest.raises(RTASchemaError, match="transfer job"):
        bus.publish_job(subject, json.dumps(broken).encode(), transfer_id)
    with pytest.raises(RTABusError, match="Nats-Msg-Id"):
        bus.publish_job(subject, _job(transfer_id), "other-id")
    after = asyncio.run(_stream_messages(nats_url, JOBS_STREAM))
    assert after == before


def test_put_cancel_writes_the_kv_bucket(bus: RTABus, nats_url: str) -> None:
    """put_cancel stores the given bytes under the transfer id."""
    transfer_id = _token("cancel")
    payload = json.dumps({
        "v": 1,
        "transfer_id": transfer_id,
        "request_id": "abc123",
        "reason": "rule deleted",
        "issued_at": "2026-11-27T09:00:00Z",
    }).encode()
    revision = bus.put_cancel(transfer_id, payload)
    assert revision >= 1
    assert asyncio.run(_kv_get(nats_url, CANCEL_BUCKET, transfer_id)) == payload


def test_kv_get_state_reads_and_misses(bus: RTABus, nats_url: str) -> None:
    """kv_get_state returns the stored bytes, and None when the key is absent."""
    transfer_id = _token("state")
    body = b'{"state":"COPY"}'
    asyncio.run(_kv_put(nats_url, STATE_BUCKET, transfer_id, body))
    assert bus.kv_get_state(transfer_id) == body
    assert bus.kv_get_state(_token("missing")) is None


def test_pull_results_acks_and_redelivers(bus: RTABus, nats_url: str) -> None:
    """An unacked result is redelivered; ack stops further delivery."""
    vo = _token("vo")
    transfer_id = _token("result")
    assert bus.pull_results(vo, batch=1, timeout=0.4, ack_wait=1) == []
    asyncio.run(_publish(nats_url, results_subject(vo), _result(transfer_id)))
    first = bus.pull_results(vo, batch=1, timeout=2, ack_wait=1)
    assert len(first) == 1
    assert first[0].data == _result(transfer_id)
    assert first[0].num_delivered == 1
    assert asyncio.run(_ack_pending(nats_url, results_consumer(vo))) == 1

    redelivered = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        batch = bus.pull_results(vo, batch=1, timeout=0.5, ack_wait=1)
        if batch:
            redelivered = batch[0]
            break
    assert redelivered is not None
    assert redelivered.num_delivered >= 2
    redelivered.ack()
    assert bus.pull_results(vo, batch=1, timeout=0.5, ack_wait=1) == []
    assert asyncio.run(_ack_pending(nats_url, results_consumer(vo))) == 0


def test_pull_schema_rejection_does_not_ack(bus: RTABus, nats_url: str) -> None:
    """A result that fails the schema stays outstanding."""
    vo = _token("bad")
    asyncio.run(_publish(nats_url, results_subject(vo), b"{}"))
    with pytest.raises(RTASchemaError, match="transfer result"):
        bus.pull_results(vo, batch=1, timeout=2, ack_wait=30)
    assert asyncio.run(_ack_pending(nats_url, results_consumer(vo))) == 1


def test_serve_tokens_replies_on_the_vo_subject(bus: RTABus, nats_url: str) -> None:
    """A request on rta.v1.token.<vo> is answered by the registered handler."""
    vo = _token("tok")
    request = b'{"v":1,"transfer_id":"job1","request_id":"abc","rse_id":"def","role":"source","min_valid_s":30}'
    reply = b'{"v":1,"token":"fresh-token","exp":1795770000}'
    received: list[bytes] = []

    def handler(data: bytes) -> bytes:
        received.append(data)
        return reply

    bus.serve_tokens(vo, handler)
    assert asyncio.run(_request(nats_url, token_subject(vo), request)) == reply
    assert received == [request]
    with pytest.raises(RTABusError, match="already being served"):
        bus.serve_tokens(vo, handler)


def test_close_stops_the_background_thread(nats_url: str) -> None:
    """close() drains the loop and a later call does not publish."""
    client = RTABus(_config(nats_url))
    client.start()
    assert any(thread.name == "rta-bus" and thread.is_alive() for thread in threading.enumerate())
    client.close()
    client.close()
    assert not any(thread.name == "rta-bus" and thread.is_alive() for thread in threading.enumerate())
    with pytest.raises(RTABusError, match="not running"):
        client.publish_job(job_subject("normal", "atlas", "SITE_B_DATADISK"), _job(_token("closed")), _token("closed"))
