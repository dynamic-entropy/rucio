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

"""Synchronous NATS facade for the RTA transfer tool.

One asyncio loop runs in a background thread. Callers use the blocking
methods on :class:`RTABus`. JetStream streams and KV buckets are provisioned
elsewhere (``rta admin init``); this module publishes and consumes them.

Subjects and buckets follow the v1 layout:

* jobs: ``rta.v1.jobs.<lane>.<vo>.<dst_rse>`` on stream ``RTA_JOBS``,
  header ``Nats-Msg-Id`` equal to ``<transfer_id>:<attempt>``
* results: ``rta.v1.results.<vo>`` on stream ``RTA_RESULTS``, durable
  pull consumer ``rucio-<vo>`` with explicit ack
* cancels: KV bucket ``rta_cancel``, key ``transfer_id``
* state: KV bucket ``rta_state``, key ``transfer_id``
* tokens: core NATS request/reply on ``rta.v1.token.<vo>``, queue group
  ``rucio-token``

Outgoing jobs and incoming results are checked against the vendored v1
JSON Schemas in ``rta_schemas/``. The source commit is recorded in
``rta_schemas/SOURCE``.

TLS material and credentials come from the ``[rta]`` section of
``rucio.cfg``. The keys are listed on :class:`RTAConfig` and in
``etc/rucio.cfg.template``.
"""

import asyncio
import atexit
import concurrent.futures
import configparser
import datetime
import functools
import json
import logging
import os
import re
import ssl
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional, TypeVar

import nats
import nats.errors
from jsonschema import Draft202012Validator, FormatChecker
from nats.js.api import AckPolicy, ConsumerConfig
from nats.js.errors import BucketNotFoundError, FetchDisconnectedError, FetchTimeoutError, KeyNotFoundError, NotFoundError

from rucio.common.config import ConfigNotFound, get_config

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from jsonschema.exceptions import ValidationError
    from nats.aio.client import Client as NATSClient
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription
    from nats.js.client import JetStreamContext
    from nats.js.kv import KeyValue

LOGGER = logging.getLogger(__name__)

_T = TypeVar("_T")

_SECTION = "rta"

JOBS_STREAM = "RTA_JOBS"
RESULTS_STREAM = "RTA_RESULTS"
CANCEL_BUCKET = "rta_cancel"
STATE_BUCKET = "rta_state"
TOKEN_QUEUE = "rucio-token"

DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_OPERATION_TIMEOUT = 10.0
DEFAULT_RECONNECT_WAIT = 2.0
DEFAULT_MAX_RECONNECT_ATTEMPTS = -1
DEFAULT_RESULT_BATCH = 100
DEFAULT_TOKEN_MIN_VALID = 3900
DEFAULT_ACK_WAIT = 120.0
DEFAULT_CLUSTER_NAME = "rta"

# Keys read from the [rta] section. etc/rucio.cfg.template documents each one.
RTA_CONFIG_KEYS: tuple[str, ...] = (
    "nats_servers",
    "nats_creds",
    "tls_ca",
    "tls_cert",
    "tls_key",
    "cluster_name",
    "connect_timeout",
    "operation_timeout",
    "reconnect_time_wait",
    "max_reconnect_attempts",
    "result_batch",
    "token_min_valid",
    "lane_by_priority",
    "lane_by_activity",
)

_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_DATE_TIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_TOKEN_FAILURE = b'{"v":1,"error":{"code":"RESPONDER_FAILED","message":"token responder failed"}}'

_SCHEMA_DIR = Path(__file__).resolve().parent / "rta_schemas" / "v1"


class RTABusError(Exception):
    """Base error raised by the RTA NATS bus."""


class RTAConnectionError(RTABusError):
    """The NATS connection could not be established or was lost."""


class RTATimeoutError(RTABusError):
    """A NATS operation exceeded its timeout."""


class RTASchemaError(RTABusError):
    """A job or result did not match the vendored v1 JSON Schema."""

    def __init__(self, kind: str, detail: str) -> None:
        """
        :param kind: ``transfer job`` or ``transfer result``.
        :param detail: Schema path that failed, without the instance value.
        """
        super().__init__(f"invalid {kind}: {detail}")
        self.kind = kind
        self.detail = detail


def _check_date_time(instance: object) -> bool:
    """Return True when ``instance`` is an RFC 3339 date-time, or not a string.

    Non-strings return True so the schema ``type`` check reports that error.
    ``Z`` is accepted on Python 3.9, whose ``fromisoformat`` rejects it.
    """
    if not isinstance(instance, str):
        return True
    if _DATE_TIME_RE.fullmatch(instance) is None:
        return False
    text = instance[:-1] + "+00:00" if instance.endswith("Z") else instance
    try:
        datetime.datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


_FORMAT_CHECKER = FormatChecker()
_FORMAT_CHECKER.checks("date-time")(_check_date_time)


def _require_token(value: str, label: str) -> None:
    """Raise when ``value`` is not a single NATS subject token."""
    if _TOKEN_RE.fullmatch(value) is None:
        raise RTABusError(f"{label} must be letters, digits, hyphen, or underscore")


def job_message_id(transfer_id: str, attempt: int) -> str:
    """
    Return the JetStream deduplication id for one submission attempt.

    A later attempt of the same transfer uses a different id, so the new
    tokens are not dropped by the duplicate window. Republishing the same
    attempt keeps the same id.

    :param transfer_id: Job ``transfer_id``. One NATS token.
    :param attempt: Job ``attempt`` (the Rucio retry count).
    :returns: ``<transfer_id>:<attempt>``.
    :raises RTABusError: The id is not a token or the attempt is negative.
    """
    _require_token(transfer_id, "transfer_id")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
        raise RTABusError("attempt must be a non-negative integer")
    return f"{transfer_id}:{attempt}"


def job_subject(lane: str, vo: str, rse: str) -> str:
    """
    Return the JetStream subject for one job.

    :param lane: ``high``, ``normal``, or ``low``.
    :param vo: Virtual organization. One NATS token.
    :param rse: Destination RSE name. One NATS token.
    :returns: ``rta.v1.jobs.<lane>.<vo>.<rse>``.
    :raises RTABusError: A component is not a safe subject token.
    """
    if lane not in ("high", "normal", "low"):
        raise RTABusError("lane must be high, normal, or low")
    _require_token(vo, "vo")
    _require_token(rse, "rse")
    return f"rta.v1.jobs.{lane}.{vo}.{rse}"


def results_subject(vo: str) -> str:
    """
    Return the results subject for one VO.

    :param vo: Virtual organization. One NATS token.
    :returns: ``rta.v1.results.<vo>``.
    """
    _require_token(vo, "vo")
    return f"rta.v1.results.{vo}"


def results_consumer(vo: str) -> str:
    """
    Return the durable results consumer name for one VO.

    :param vo: Virtual organization. One NATS token.
    :returns: ``rucio-<vo>``.
    """
    _require_token(vo, "vo")
    return f"rucio-{vo}"


def token_subject(vo: str) -> str:
    """
    Return the token request subject for one VO.

    :param vo: Virtual organization. One NATS token.
    :returns: ``rta.v1.token.<vo>``.
    """
    _require_token(vo, "vo")
    return f"rta.v1.token.{vo}"


@functools.cache
def _validator(filename: str) -> Draft202012Validator:
    """Load and cache one vendored schema."""
    path = _SCHEMA_DIR / filename
    try:
        payload = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RTABusError(f"vendored RTA schema {filename} is missing") from exc
    schema = json.loads(payload)
    if not isinstance(schema, dict):
        raise RTABusError(f"vendored RTA schema {filename} is not an object")
    return Draft202012Validator(schema, format_checker=_FORMAT_CHECKER)


def _schema_detail(error: "ValidationError") -> str:
    """Describe a schema failure without echoing the instance.

    Job and result documents carry bearer tokens. The validator message
    repeats the offending value, so only the path and keyword are kept.
    """
    location = error.json_path or "$"
    return f"{location} failed {error.validator}"


def _validate(payload: bytes, kind: str, filename: str) -> dict[str, object]:
    """Parse ``payload`` and check it against one vendored schema."""
    if not isinstance(payload, bytes):
        raise RTABusError(f"{kind} payload must be bytes")
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RTASchemaError(kind, "payload is not JSON") from exc
    if not isinstance(document, dict):
        raise RTASchemaError(kind, "payload is not a JSON object")
    errors = sorted(_validator(filename).iter_errors(document), key=lambda item: item.json_path or "")
    if errors:
        detail = "; ".join(_schema_detail(error) for error in errors[:5])
        raise RTASchemaError(kind, detail)
    return document


def validate_job(payload: bytes) -> dict[str, object]:
    """
    Check a TransferJob against the vendored v1 schema.

    :param payload: JSON object encoded as UTF-8 bytes. It is not rewritten.
    :returns: The parsed object.
    :raises RTASchemaError: The payload is not a valid TransferJob.
    """
    return _validate(payload, "transfer job", "transfer_job.json")


def validate_cancel(payload: bytes) -> dict[str, object]:
    """
    Check a Cancel tombstone against the vendored v1 schema.

    :param payload: JSON object encoded as UTF-8 bytes.
    :returns: The parsed object.
    :raises RTASchemaError: The payload is not a valid Cancel.
    """
    return _validate(payload, "cancel", "cancel.json")


def validate_token_request(payload: bytes) -> dict[str, object]:
    """
    Check a TokenRequest against the vendored v1 schema.

    :param payload: JSON object encoded as UTF-8 bytes.
    :returns: The parsed object.
    :raises RTASchemaError: The payload is not a valid TokenRequest.
    """
    return _validate(payload, "token request", "token_request.json")


def validate_token_reply(payload: bytes) -> dict[str, object]:
    """
    Check a TokenReply against the vendored v1 schema.

    :param payload: JSON object encoded as UTF-8 bytes.
    :returns: The parsed object.
    :raises RTASchemaError: The payload is not a valid TokenReply.
    """
    return _validate(payload, "token reply", "token_reply.json")


def validate_result(payload: bytes) -> dict[str, object]:
    """
    Check a TransferResult against the vendored v1 schema.

    :param payload: JSON object encoded as UTF-8 bytes. It is not rewritten.
    :returns: The parsed object.
    :raises RTASchemaError: The payload is not a valid TransferResult.
    """
    return _validate(payload, "transfer result", "transfer_result.json")


def _optional(parser: configparser.ConfigParser, option: str) -> Optional[str]:
    """Return a stripped ``[rta]`` option, or None when it is absent or blank."""
    if not parser.has_section(_SECTION) or not parser.has_option(_SECTION, option):
        return None
    value = parser.get(_SECTION, option).strip()
    if not value:
        return None
    return value


def _optional_int(parser: configparser.ConfigParser, option: str, default: int) -> int:
    """Return an integer ``[rta]`` option, or ``default`` when it is absent."""
    raw = _optional(parser, option)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RTABusError(f"[rta] {option} must be an integer") from exc


def _optional_float(parser: configparser.ConfigParser, option: str, default: float) -> float:
    """Return a float ``[rta]`` option, or ``default`` when it is absent."""
    raw = _optional(parser, option)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RTABusError(f"[rta] {option} must be a number") from exc


def _split_servers(raw: str) -> list[str]:
    """Split a comma-separated NATS URL list."""
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass
class RTAConfig:
    """Connection settings for the RTA NATS bus.

    ``nats_servers`` is required. The remaining fields have defaults.
    ``lane_by_priority``, ``lane_by_activity``, and ``token_min_valid`` are
    stored for the transfer tool and the token handler; the bus does not
    route or mint tokens with them.

    :param nats_servers: NATS URLs, for example ``nats://127.0.0.1:4222``.
        ``tls://`` turns on TLS. Put credentials in ``nats_creds``, not in the URL.
    :param nats_creds: Path to a NATS credentials file.
    :param tls_ca: CA bundle used to verify the server certificate.
    :param tls_cert: Client certificate for mutual TLS. Requires ``tls_key``.
    :param tls_key: Client private key for mutual TLS. Requires ``tls_cert``.
    :param cluster_name: Logical name. Later wiring uses ``rta://<cluster_name>``.
    :param connect_timeout: Seconds to wait for the first TCP connection.
    :param operation_timeout: Seconds for publish, KV, pull, and subscribe calls.
    :param reconnect_time_wait: Seconds between reconnect attempts.
    :param max_reconnect_attempts: Retries after a drop. ``-1`` retries forever.
    :param result_batch: Default ``pull_results`` batch size.
    :param token_min_valid: Seconds of token lifetime the responder should require.
    :param lane_by_priority: Reserved lane map. Not used for routing here.
    :param lane_by_activity: Reserved lane map. Not used for routing here.
    """

    nats_servers: list[str]
    nats_creds: Optional[str] = None
    tls_ca: Optional[str] = None
    tls_cert: Optional[str] = None
    tls_key: Optional[str] = None
    cluster_name: str = DEFAULT_CLUSTER_NAME
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    operation_timeout: float = DEFAULT_OPERATION_TIMEOUT
    reconnect_time_wait: float = DEFAULT_RECONNECT_WAIT
    max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS
    result_batch: int = DEFAULT_RESULT_BATCH
    token_min_valid: int = DEFAULT_TOKEN_MIN_VALID
    lane_by_priority: Optional[str] = None
    lane_by_activity: Optional[str] = None

    def __post_init__(self) -> None:
        """Reject a configuration that cannot be used to connect."""
        servers = [server.strip() for server in self.nats_servers if server.strip()]
        if not servers:
            raise RTABusError("[rta] nats_servers is required")
        for server in servers:
            if "://" not in server:
                raise RTABusError("[rta] nats_servers contains an entry that is not a NATS URL")
        self.nats_servers = servers
        _require_token(self.cluster_name, "cluster_name")
        if (self.tls_cert is None) != (self.tls_key is None):
            raise RTABusError("[rta] tls_cert and tls_key must be set together")
        if self.connect_timeout <= 0:
            raise RTABusError("[rta] connect_timeout must be positive")
        if self.operation_timeout <= 0:
            raise RTABusError("[rta] operation_timeout must be positive")
        if self.reconnect_time_wait < 0:
            raise RTABusError("[rta] reconnect_time_wait must be zero or positive")
        if self.max_reconnect_attempts < -1:
            raise RTABusError("[rta] max_reconnect_attempts must be -1 or greater")
        if self.result_batch < 1:
            raise RTABusError("[rta] result_batch must be at least 1")
        if self.token_min_valid < 1:
            raise RTABusError("[rta] token_min_valid must be at least 1")

    @classmethod
    def from_parser(cls, parser: configparser.ConfigParser) -> "RTAConfig":
        """
        Build a configuration from a ``[rta]`` section.

        :param parser: Parser that contains the section.
        :returns: A validated configuration.
        :raises RTABusError: The section or ``nats_servers`` is missing, or a value is invalid.
        """
        if not parser.has_section(_SECTION):
            raise RTABusError("configuration has no [rta] section")
        try:
            raw_servers = parser.get(_SECTION, "nats_servers")
        except configparser.NoOptionError as exc:
            raise RTABusError("[rta] nats_servers is required") from exc
        return cls(
            nats_servers=_split_servers(raw_servers),
            nats_creds=_optional(parser, "nats_creds"),
            tls_ca=_optional(parser, "tls_ca"),
            tls_cert=_optional(parser, "tls_cert"),
            tls_key=_optional(parser, "tls_key"),
            cluster_name=_optional(parser, "cluster_name") or DEFAULT_CLUSTER_NAME,
            connect_timeout=_optional_float(parser, "connect_timeout", DEFAULT_CONNECT_TIMEOUT),
            operation_timeout=_optional_float(parser, "operation_timeout", DEFAULT_OPERATION_TIMEOUT),
            reconnect_time_wait=_optional_float(parser, "reconnect_time_wait", DEFAULT_RECONNECT_WAIT),
            max_reconnect_attempts=_optional_int(parser, "max_reconnect_attempts", DEFAULT_MAX_RECONNECT_ATTEMPTS),
            result_batch=_optional_int(parser, "result_batch", DEFAULT_RESULT_BATCH),
            token_min_valid=_optional_int(parser, "token_min_valid", DEFAULT_TOKEN_MIN_VALID),
            lane_by_priority=_optional(parser, "lane_by_priority"),
            lane_by_activity=_optional(parser, "lane_by_activity"),
        )

    @classmethod
    def from_config(cls) -> "RTAConfig":
        """
        Read :class:`RTAConfig` from the process Rucio configuration.

        The config table is not consulted. ``[rta]`` lives in ``rucio.cfg``.

        :returns: A validated configuration.
        :raises RTABusError: The configuration file or the ``[rta]`` section is missing.
        """
        try:
            parser = get_config()
        except ConfigNotFound as exc:
            raise RTABusError("Rucio configuration was not found") from exc
        return cls.from_parser(parser)


def _tls_context(config: RTAConfig) -> Optional[ssl.SSLContext]:
    """Build a TLS context when the configuration or a ``tls://`` URL asks for one."""
    wants_tls = any(server.startswith("tls://") for server in config.nats_servers)
    if config.tls_ca or config.tls_cert or wants_tls:
        context = ssl.create_default_context(cafile=config.tls_ca)
        if config.tls_cert and config.tls_key:
            context.load_cert_chain(config.tls_cert, config.tls_key)
        return context
    return None


class PublishAck:
    """JetStream acknowledgement for one published job."""

    def __init__(self, stream: str, seq: int, duplicate: bool) -> None:
        """
        :param stream: Stream that stored the message.
        :param seq: Stream sequence. A duplicate publish repeats the original sequence.
        :param duplicate: True when the server dropped the publish because ``Nats-Msg-Id`` was already seen.
        """
        self.stream = stream
        self.seq = seq
        self.duplicate = duplicate


class PulledResult:
    """One explicitly acknowledged result delivery.

    ``ack`` and ``nak`` run on the bus loop. The message stays outstanding
    until one of them is called, and the server redelivers it after the
    consumer ack wait.
    """

    def __init__(self, bus: "RTABus", message: "Msg", num_delivered: int) -> None:
        """
        :param bus: Bus whose loop owns ``message``.
        :param message: JetStream message. The payload has already passed the result schema.
        :param num_delivered: How many times the server has delivered this message.
        """
        self.subject = message.subject
        self.data = message.data
        self.headers = dict(message.headers) if message.headers else {}
        self.num_delivered = num_delivered
        self._bus = bus
        self._message = message

    def ack(self) -> None:
        """Acknowledge the delivery so the server will not redeliver it."""
        self._bus._settle(self._message, nak=False)

    def nak(self) -> None:
        """Negatively acknowledge the delivery so the server redelivers it."""
        self._bus._settle(self._message, nak=True)


class RTABus:
    """Blocking facade over one NATS connection and one asyncio loop.

    Construct it with :class:`RTAConfig`, call :meth:`start`, and call
    :meth:`close` on shutdown (or use it as a context manager). A dropped
    connection is retried according to ``max_reconnect_attempts``. Operations
    wait up to ``connect_timeout`` for a reconnect that is already in progress,
    then fail with :class:`RTATimeoutError`. The next call tries again.

    Token handlers run in a worker thread so a slow mint does not stall
    JetStream acks on the loop. Handlers must be thread-safe and must not
    call back into the bus.

    The loop thread is a daemon. :meth:`close` is idempotent and registered
    with :func:`atexit.register`. After :func:`os.fork`, the child drops the
    inherited connection and the next call starts a new one.
    Fetches for one VO share one durable pull subscription and are serialized
    with an asyncio lock on the bus loop.
    """

    def __init__(self, config: RTAConfig) -> None:
        """
        :param config: Connection settings. Not read from disk when passed in.
        """
        self._config = config
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._shutdown_requested = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop: Optional[asyncio.Event] = None
        self._conn_ready: Optional[asyncio.Event] = None
        self._start_error: Optional[Exception] = None
        self._closed = False
        self._close_done = False
        self._restart_after_fork = False
        self._nc: Optional[NATSClient] = None
        self._js: Optional[JetStreamContext] = None
        self._result_subs: dict[str, "JetStreamContext.PullSubscription"] = {}
        self._pull_locks: dict[str, asyncio.Lock] = {}
        self._token_subs: dict[str, Subscription] = {}
        atexit.register(self.close)
        os.register_at_fork(after_in_child=self._after_fork_child)

    def __enter__(self) -> "RTABus":
        """Start the bus and return it."""
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close the bus."""
        self.close()

    def start(self) -> None:
        """
        Connect to NATS on the background loop.

        :raises RTABusError: The bus was already started or is closed.
        :raises RTAConnectionError: The server rejected the connection.
        :raises RTATimeoutError: The connection did not complete in time.
        """
        with self._lock:
            if self._closed:
                raise RTABusError("RTA bus is closed")
            if self._thread is not None:
                raise RTABusError("RTA bus is already started")
            self._ready.clear()
            self._start_error = None
            self._shutdown_requested.clear()
            thread = threading.Thread(target=self._thread_main, name="rta-bus", daemon=True)
            self._thread = thread
            thread.start()
        wait = self._config.connect_timeout + 1
        if not self._ready.wait(wait):
            self.close()
            raise RTATimeoutError("timed out connecting to NATS")
        if self._start_error is not None:
            error = self._start_error
            self.close()
            raise error

    def close(self, timeout: Optional[float] = None) -> None:
        """
        Stop the loop, drain the connection, and join the background thread.

        Pending tasks are cancelled and awaited on that loop before it closes.
        A second call does nothing. :func:`atexit` calls this on process exit.
        Calling it from the bus thread raises instead of joining itself.

        :param timeout: Seconds to wait for the thread. Defaults to ``operation_timeout``.
        :raises RTABusError: Called from the bus thread.
        """
        self._reject_loop_thread()
        with self._lock:
            if self._close_done:
                return
            thread = self._thread
            self._closed = True
            self._restart_after_fork = False
            self._shutdown_requested.set()
        if thread is None:
            self._close_done = True
            return
        limit = self._config.operation_timeout if timeout is None else timeout
        if limit <= 0:
            limit = self._config.operation_timeout
        self._signal_stop()
        thread.join(limit)
        if thread.is_alive():
            self._force_stop_loop()
            thread.join(limit)
        self._close_done = True

    def publish_job(self, subject: str, payload: bytes, msg_id: str, timeout: Optional[float] = None) -> PublishAck:
        """
        Publish one TransferJob and wait for the JetStream PubAck.

        The payload is validated and then published unchanged. ``msg_id`` is
        sent as ``Nats-Msg-Id`` and must equal ``<transfer_id>:<attempt>``.
        ``PublishAck.duplicate`` is true when JetStream dropped this body
        because that id was already published. The subject must be
        ``rta.v1.jobs.<lane>.<vo>.<destination rse>`` for that job.

        :param subject: JetStream subject.
        :param payload: UTF-8 JSON TransferJob.
        :param msg_id: Deduplication id. ``<transfer_id>:<attempt>``.
        :param timeout: Seconds to wait for the PubAck.
        :returns: The publish acknowledgement, including ``duplicate``.
        :raises RTASchemaError: The payload is not a TransferJob.
        :raises RTABusError: The id or subject does not match the payload, or the bus is stopped.
        :raises RTATimeoutError: The PubAck did not arrive in time.
        """
        self._ensure_started()
        document = validate_job(payload)
        transfer_id = document.get("transfer_id")
        attempt = document.get("attempt")
        if not isinstance(transfer_id, str) or isinstance(attempt, bool) or not isinstance(attempt, int):
            raise RTASchemaError("transfer job", "transfer_id or attempt has the wrong type")
        canonical = job_message_id(transfer_id, attempt)
        if msg_id != canonical:
            raise RTABusError("Nats-Msg-Id must equal transfer_id and attempt")
        expected = _job_subject_for(document)
        if subject != expected:
            raise RTABusError("job subject does not match lane, vo, and destination RSE")
        limit = self._limit(timeout)
        ack = self._call(self._publish_job(subject, payload, msg_id, limit), limit + 1)
        return ack

    def put_cancel(self, transfer_id: str, payload: bytes, timeout: Optional[float] = None) -> int:
        """
        Write a cancel tombstone to the ``rta_cancel`` bucket.

        :param transfer_id: KV key. One NATS token.
        :param payload: Cancel JSON bytes. They must match ``cancel.json``.
        :param timeout: Seconds to wait for the write.
        :returns: The new KV revision.
        :raises RTASchemaError: The payload is not a Cancel, or its ``transfer_id`` is not the key.
        :raises RTABusError: The key is not a NATS token, the bucket is missing, or the bus is stopped.
        """
        self._ensure_started()
        _require_token(transfer_id, "transfer_id")
        document = validate_cancel(payload)
        if document.get("transfer_id") != transfer_id:
            raise RTASchemaError("cancel", "transfer_id does not equal the KV key")
        limit = self._limit(timeout)
        return self._call(self._put_cancel(transfer_id, payload), limit + 1)

    def kv_get_state(self, transfer_id: str, timeout: Optional[float] = None) -> Optional[bytes]:
        """
        Read one value from the ``rta_state`` bucket.

        :param transfer_id: KV key. One NATS token.
        :param timeout: Seconds to wait for the read.
        :returns: The stored bytes, or None when the key is absent.
        :raises RTABusError: The bucket is missing or the bus is stopped.
        """
        self._ensure_started()
        _require_token(transfer_id, "transfer_id")
        limit = self._limit(timeout)
        return self._call(self._kv_get_state(transfer_id), limit + 1)

    def pull_results(
        self,
        vo: str,
        batch: Optional[int] = None,
        timeout: Optional[float] = None,
        ack_wait: Optional[float] = None,
        max_deliver: Optional[int] = None,
    ) -> list[PulledResult]:
        """
        Pull a batch of results with an explicit-ack durable consumer.

        Fetches for one VO are serialized on the bus loop. They share one
        pull subscription for durable ``rucio-<vo>``, so two callers cannot
        interleave deliveries on its pending queue.

        If that consumer already exists, its ``ack_wait``, ``max_deliver``,
        and filter subject are compared with this call. A difference is
        written with a consumer update. When the server rejects the update,
        the call raises and does not keep using the old settings.

        Each message is validated on its own. A schema failure is logged with
        a redacted body and terminated, so it cannot stall the consumer.
        Valid messages from the same fetch are still returned.

        An empty batch is returned when the fetch times out. A disconnect
        during the fetch raises :class:`RTAConnectionError`.

        :param vo: Virtual organization. One NATS token.
        :param batch: Maximum messages. Defaults to ``result_batch``.
        :param timeout: Seconds to wait for a message.
        :param ack_wait: Consumer ack wait in seconds. Defaults to 120.
        :param max_deliver: Maximum deliveries. ``-1`` means unlimited.
        :returns: Valid results. Each one must be acked or naked.
        :raises RTAConnectionError: The connection dropped during the fetch.
        :raises RTABusError: The consumer cannot be created or updated, or the bus is stopped.
        """
        self._ensure_started()
        _require_token(vo, "vo")
        size = self._config.result_batch if batch is None else batch
        if size < 1:
            raise RTABusError("result batch must be at least 1")
        wait = DEFAULT_ACK_WAIT if ack_wait is None else ack_wait
        if wait <= 0:
            raise RTABusError("ack_wait must be positive")
        deliver = -1 if max_deliver is None else max_deliver
        if deliver == 0 or deliver < -1:
            raise RTABusError("max_deliver must be -1 or a positive integer")
        limit = self._limit(timeout)
        return self._call(self._pull_results(vo, size, limit, wait, deliver), limit + 1)

    def serve_tokens(self, vo: str, handler: "Callable[[bytes], bytes]", timeout: Optional[float] = None) -> None:
        """
        Answer token requests on ``rta.v1.token.<vo>``.

        The subscription uses queue group ``rucio-token``. ``handler`` is
        called with the request bytes and its return value is the reply.
        It runs on a worker thread. A handler exception becomes a small
        error reply and is not re-raised here.

        :param vo: Virtual organization. One NATS token.
        :param handler: Maps a request body to a reply body.
        :param timeout: Seconds to wait for the subscription to flush.
        :raises RTABusError: This VO is already being served, or the bus is stopped.
        """
        self._ensure_started()
        _require_token(vo, "vo")
        limit = self._limit(timeout)
        self._call(self._serve_tokens(vo, handler), limit + 1)

    def _ensure_started(self) -> None:
        """Start the bus when a fork discarded the parent connection."""
        if self._restart_after_fork:
            self._restart_after_fork = False
            self._closed = False
            self._close_done = False
            self._thread = None
            self.start()
            return
        self._require_running()

    def _reject_loop_thread(self) -> None:
        """Raise when the caller is the bus thread. Waiting there deadlocks the loop."""
        thread = self._thread
        if thread is not None and threading.current_thread() is thread:
            raise RTABusError("RTA bus methods cannot be called from the bus thread")

    def _require_running(self) -> None:
        """Raise when start() has not completed or close() has begun."""
        thread = self._thread
        loop = self._loop
        if self._closed or thread is None or loop is None or not thread.is_alive() or not loop.is_running():
            raise RTABusError("RTA bus is not running")

    def _call(self, coro: "Coroutine[None, None, _T]", timeout: Optional[float] = None) -> _T:
        """Run ``coro`` on the bus loop and wait."""
        if self._thread is not None and threading.current_thread() is self._thread:
            coro.close()
            raise RTABusError("RTA bus methods cannot be called from the bus thread")
        loop = self._loop
        if self._closed or loop is None or not loop.is_running():
            coro.close()
            raise RTABusError("RTA bus is not running")
        try:
            limit = self._limit(timeout)
        except RTATimeoutError:
            coro.close()
            raise
        # The NATS call uses ``limit``. This wait is a backstop so a slow
        # server timeout is reported by NATS rather than cancelled underneath it.
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(limit + 1)
        except RTABusError:
            raise
        except nats.errors.TimeoutError as exc:
            raise RTATimeoutError(str(exc)) from exc
        except (nats.errors.ConnectionClosedError, nats.errors.NoServersError, nats.errors.ConnectionReconnectingError) as exc:
            raise RTAConnectionError(str(exc)) from exc
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise RTATimeoutError(f"timed out after {limit} seconds") from exc
        except Exception as exc:
            raise RTABusError(str(exc)) from exc

    def _signal_stop(self) -> None:
        """Ask the loop to leave ``_serve``."""
        loop = self._loop
        stop = self._stop
        if loop is not None and stop is not None and loop.is_running():
            loop.call_soon_threadsafe(stop.set)

    def _force_stop_loop(self) -> None:
        """Stop the loop when drain does not finish."""
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

    def _thread_main(self) -> None:
        """Own the asyncio loop for the life of the thread."""
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._serve())
        except RuntimeError:
            LOGGER.debug("RTA bus event loop stopped")
        finally:
            _stop_remaining_tasks(loop)

    async def _serve(self) -> None:
        """Connect, wait for shutdown, then drain."""
        self._stop = asyncio.Event()
        self._conn_ready = asyncio.Event()
        if self._shutdown_requested.is_set():
            self._start_error = RTABusError("RTA bus closed before connect")
            self._ready.set()
            return
        try:
            await self._connect()
        except Exception as exc:
            LOGGER.warning("RTA NATS connection failed: %s", exc)
            if isinstance(exc, RTABusError):
                self._start_error = exc
            else:
                self._start_error = RTAConnectionError(f"failed to connect to NATS: {exc}")
            await self._shutdown()
            self._ready.set()
            return
        self._conn_ready.set()
        self._ready.set()
        if self._shutdown_requested.is_set():
            self._stop.set()
        await self._stop.wait()
        await self._shutdown()

    async def _connect(self) -> None:
        """Open the NATS connection and the JetStream context."""
        tls_context = _tls_context(self._config)
        self._nc = await nats.connect(
            servers=self._config.nats_servers,
            name="rucio-rta",
            allow_reconnect=True,
            connect_timeout=self._config.connect_timeout,
            reconnect_time_wait=self._config.reconnect_time_wait,
            max_reconnect_attempts=self._config.max_reconnect_attempts,
            drain_timeout=self._config.operation_timeout,
            user_credentials=self._config.nats_creds,
            tls=tls_context,
            error_cb=self._on_error,
            disconnected_cb=self._on_disconnect,
            reconnected_cb=self._on_reconnect,
            closed_cb=self._on_closed,
        )
        self._js = self._nc.jetstream(timeout=self._config.operation_timeout)

    async def _on_error(self, exc: Exception) -> None:
        """Log a NATS client error. The payload is not included."""
        LOGGER.warning("RTA NATS client error: %s", exc)

    async def _on_disconnect(self) -> None:
        """Mark the connection down so operations wait for reconnect."""
        if self._conn_ready is not None:
            self._conn_ready.clear()
        LOGGER.warning("RTA NATS connection lost")

    async def _on_reconnect(self) -> None:
        """Release operations that were waiting for the connection."""
        if self._conn_ready is not None:
            self._conn_ready.set()
        LOGGER.info("RTA NATS connection restored")

    async def _on_closed(self) -> None:
        """The client gave up reconnecting or was closed."""
        if self._conn_ready is not None:
            self._conn_ready.clear()
        LOGGER.warning("RTA NATS connection closed")

    async def _ensure_connected(self) -> None:
        """Wait out an in-progress reconnect, or raise when the client is gone."""
        client = self._nc
        ready = self._conn_ready
        if client is None or ready is None or client.is_closed:
            raise RTAConnectionError("RTA NATS connection is closed")
        if client.is_connected:
            return
        if not client.is_reconnecting:
            raise RTAConnectionError("RTA NATS connection is down")
        try:
            await asyncio.wait_for(ready.wait(), self._config.connect_timeout)
        except asyncio.TimeoutError as exc:
            raise RTATimeoutError("timed out waiting for NATS to reconnect") from exc

    def _limit(self, timeout: Optional[float]) -> float:
        """Return the positive timeout for one call."""
        limit = self._config.operation_timeout if timeout is None else timeout
        if limit <= 0:
            raise RTATimeoutError("operation timeout must be positive")
        return limit

    async def _publish_job(self, subject: str, payload: bytes, msg_id: str, timeout: float) -> PublishAck:
        """Publish after the connection is usable and return the PubAck."""
        await self._ensure_connected()
        jetstream = self._require_jetstream()
        ack = await jetstream.publish(
            subject,
            payload,
            timeout=timeout,
            headers={"Nats-Msg-Id": msg_id},
        )
        return PublishAck(stream=ack.stream, seq=ack.seq, duplicate=bool(ack.duplicate))

    async def _put_cancel(self, transfer_id: str, payload: bytes) -> int:
        """Write the cancel bucket."""
        await self._ensure_connected()
        bucket = await self._bucket(CANCEL_BUCKET)
        return await bucket.put(transfer_id, payload)

    async def _kv_get_state(self, transfer_id: str) -> Optional[bytes]:
        """Read the state bucket. A missing key is None."""
        await self._ensure_connected()
        bucket = await self._bucket(STATE_BUCKET)
        try:
            entry = await bucket.get(transfer_id)
        except KeyNotFoundError:
            return None
        return entry.value

    async def _bucket(self, name: str) -> "KeyValue":
        """Bind to a KV bucket that ``rta admin init`` already created."""
        jetstream = self._require_jetstream()
        try:
            return await jetstream.key_value(name)
        except BucketNotFoundError as exc:
            raise RTABusError(f"KV bucket {name} does not exist") from exc

    async def _pull_results(
        self,
        vo: str,
        batch: int,
        timeout: float,
        ack_wait: float,
        max_deliver: int,
    ) -> list[PulledResult]:
        """Fetch one batch. Terminate invalid messages and return the valid ones."""
        await self._ensure_connected()
        lock = self._pull_locks.get(vo)
        if lock is None:
            lock = asyncio.Lock()
            self._pull_locks[vo] = lock
        async with lock:
            subscription = await self._results_subscription(vo, ack_wait, max_deliver)
            try:
                messages = await subscription.fetch(batch, timeout=timeout)
            except FetchDisconnectedError as exc:
                raise RTAConnectionError("NATS disconnected while pulling results") from exc
            except (FetchTimeoutError, nats.errors.TimeoutError):
                # An idle consumer reports timeout rather than an empty batch.
                return []
            pulled: list[PulledResult] = []
            for message in messages:
                try:
                    validate_result(message.data)
                except RTASchemaError as exc:
                    LOGGER.warning(
                        "RTA result on %s seq=%s failed schema (%s); terminating it. body=%s",
                        message.subject,
                        _stream_sequence(message),
                        exc.detail,
                        _redacted_body(message.data),
                    )
                    await message.term()
                    continue
                try:
                    delivered = message.metadata.num_delivered
                except nats.errors.NotJSMessageError as exc:
                    raise RTABusError("result message has no JetStream metadata") from exc
                pulled.append(PulledResult(self, message, delivered))
            return pulled

    async def _results_subscription(
        self,
        vo: str,
        ack_wait: float,
        max_deliver: int,
    ) -> "JetStreamContext.PullSubscription":
        """Bind the durable consumer, creating or updating it to the requested config."""
        jetstream = self._require_jetstream()
        durable = results_consumer(vo)
        subject = results_subject(vo)
        desired = ConsumerConfig(
            name=durable,
            durable_name=durable,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=ack_wait,
            max_deliver=max_deliver,
            filter_subject=subject,
        )
        try:
            info = await jetstream.consumer_info(RESULTS_STREAM, durable)
        except NotFoundError:
            info = None
        if info is None or _consumer_differs(info.config, desired):
            try:
                await jetstream.add_consumer(RESULTS_STREAM, config=desired)
            except Exception as exc:
                current = "absent" if info is None else (
                    f"ack_wait={info.config.ack_wait} max_deliver={info.config.max_deliver} "
                    f"filter={info.config.filter_subject!r}"
                )
                raise RTABusError(
                    f"result consumer {durable} ({current}) does not match "
                    f"ack_wait={ack_wait} max_deliver={max_deliver} filter={subject!r}"
                ) from exc
        existing = self._result_subs.get(vo)
        if existing is not None:
            return existing
        subscription = await jetstream.pull_subscribe(
            subject,
            durable=durable,
            stream=RESULTS_STREAM,
        )
        self._result_subs[vo] = subscription
        return subscription

    async def _serve_tokens(self, vo: str, handler: "Callable[[bytes], bytes]") -> None:
        """Subscribe to the token subject and flush so the interest is registered."""
        if vo in self._token_subs:
            raise RTABusError(f"token subject for {vo} is already being served")
        await self._ensure_connected()
        client = self._nc
        if client is None:
            raise RTAConnectionError("RTA NATS connection is closed")
        subject = token_subject(vo)

        async def _callback(message: "Msg") -> None:
            await self._handle_token(message, handler)

        subscription = await client.subscribe(subject, queue=TOKEN_QUEUE, cb=_callback)
        # nats-py annotates flush's timeout as int. The implementation accepts a float.
        await client.flush(timeout=self._config.operation_timeout)  # type: ignore[arg-type]
        self._token_subs[vo] = subscription

    async def _handle_token(self, message: "Msg", handler: "Callable[[bytes], bytes]") -> None:
        """Run the handler off the loop and reply. Failures get a generic error body."""
        if not message.reply:
            LOGGER.warning("RTA token request on %s had no reply subject", message.subject)
            return
        try:
            validate_token_request(message.data)
        except RTASchemaError as exc:
            LOGGER.warning("RTA token request on %s failed schema: %s", message.subject, exc.detail)
            reply: object = _TOKEN_FAILURE
        else:
            try:
                reply = await asyncio.get_running_loop().run_in_executor(None, handler, message.data)
            except Exception:
                LOGGER.warning("RTA token responder failed on %s", message.subject)
                reply = _TOKEN_FAILURE
        if not isinstance(reply, bytes):
            LOGGER.warning("RTA token responder on %s did not return bytes", message.subject)
            reply = _TOKEN_FAILURE
        else:
            try:
                validate_token_reply(reply)
            except RTASchemaError as exc:
                LOGGER.warning("RTA token reply on %s failed schema: %s", message.subject, exc.detail)
                reply = _TOKEN_FAILURE
        try:
            await message.respond(reply)
        except Exception:
            LOGGER.warning("RTA token reply failed on %s", message.subject)

    def _settle(self, message: "Msg", nak: bool) -> None:
        """Ack or nak one result on the bus loop."""
        self._call(self._settle_async(message, nak))

    async def _settle_async(self, message: "Msg", nak: bool) -> None:
        """Ack or nak one JetStream message."""
        if nak:
            await message.nak()
            return
        await message.ack()

    async def _shutdown(self) -> None:
        """Unsubscribe token handlers and drain the connection."""
        subscriptions = list(self._token_subs.values())
        self._token_subs.clear()
        self._result_subs.clear()
        for subscription in subscriptions:
            try:
                await subscription.unsubscribe()
            except Exception:
                LOGGER.warning("RTA token unsubscribe failed")
        client = self._nc
        self._nc = None
        self._js = None
        if client is None or client.is_closed:
            return
        try:
            await client.drain()
        except Exception as exc:
            LOGGER.warning("RTA NATS drain failed; closing: %s", exc)
            try:
                await client.close()
            except Exception as close_error:
                LOGGER.warning("RTA NATS close failed: %s", close_error)

    def _after_fork_child(self) -> None:
        """Drop the parent's loop and connection. The next call starts a new one.

        This runs in the child with only the forking thread alive. It does not
        take the bus lock or call into the NATS client, because those locks may
        have been held by a thread that no longer exists.
        """
        client = self._nc
        self._nc = None
        self._js = None
        self._loop = None
        self._thread = None
        self._stop = None
        self._conn_ready = None
        self._start_error = None
        self._closed = False
        self._close_done = False
        self._restart_after_fork = True
        self._result_subs = {}
        self._pull_locks = {}
        self._token_subs = {}
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._shutdown_requested = threading.Event()
        if client is not None:
            _close_inherited_socket(client)

    def _require_jetstream(self) -> "JetStreamContext":
        """Return the JetStream context or raise when it is gone."""
        jetstream = self._js
        if jetstream is None:
            raise RTAConnectionError("RTA JetStream context is closed")
        return jetstream


def _stream_sequence(message: "Msg") -> str:
    """Return the stream sequence, or ``?`` when the message has no JetStream metadata."""
    try:
        return str(message.metadata.sequence.stream)
    except nats.errors.NotJSMessageError:
        return "?"


def _redacted_body(payload: bytes) -> str:
    """Return the JSON shape with every string replaced, so tokens are not logged."""
    try:
        document = json.loads(payload)
    except json.JSONDecodeError:
        return "<not json>"
    return json.dumps(_redact(document), separators=(",", ":"))


def _redact(value: object) -> object:
    """Replace strings and leave numbers, booleans, and nulls in place."""
    if isinstance(value, dict):
        redacted: dict[str, object] = {}
        for key, item in value.items():
            redacted[str(key)] = _redact(item)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        return "***"
    return value


def _unlimited(value: Optional[int]) -> int:
    """Treat a missing or negative max_deliver as unlimited."""
    if value is None or value < 0:
        return -1
    return value


def _policy_name(value: object) -> str:
    """Return the ack policy name without the enum wrapper."""
    raw = getattr(value, "value", value)
    return str(raw)


def _consumer_differs(current: "ConsumerConfig", desired: "ConsumerConfig") -> bool:
    """Return whether the server consumer does not match the requested pull settings."""
    current_wait = 0.0 if current.ack_wait is None else current.ack_wait
    desired_wait = 0.0 if desired.ack_wait is None else desired.ack_wait
    if abs(current_wait - desired_wait) > 0.05:
        return True
    if _unlimited(current.max_deliver) != _unlimited(desired.max_deliver):
        return True
    if (current.filter_subject or "") != (desired.filter_subject or ""):
        return True
    if _policy_name(current.ack_policy) != "explicit":
        return True
    return False


def _close_inherited_socket(client: object) -> None:
    """Close the NATS socket duplicated into a forked child."""
    transport = getattr(client, "_transport", None)
    writer = getattr(transport, "_io_writer", None)
    raw = getattr(writer, "transport", None)
    sock = raw.get_extra_info("socket") if raw is not None else None
    if sock is None:
        return
    try:
        sock.close()
    except OSError:
        LOGGER.debug("RTA child could not close the inherited NATS socket")


def _stop_remaining_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel leftover NATS tasks and wait for them before closing the loop."""
    if loop.is_closed():
        return
    pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except RuntimeError:
            LOGGER.debug("RTA bus could not finish pending NATS tasks")
    if not loop.is_closed():
        loop.close()


def _job_subject_for(document: dict[str, object]) -> str:
    """Build the subject the job document itself implies."""
    lane = document.get("lane")
    vo = document.get("vo")
    destination = document.get("destination")
    if not isinstance(lane, str) or not isinstance(vo, str) or not isinstance(destination, dict):
        raise RTASchemaError("transfer job", "lane, vo, or destination has the wrong type")
    rse = destination.get("rse")
    if not isinstance(rse, str):
        raise RTASchemaError("transfer job", "destination.rse has the wrong type")
    return job_subject(lane, vo, rse)
