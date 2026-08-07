from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import datetime
from threading import Lock
from typing import Any

import clickhouse_connect
import sentry_sdk
from clickhouse_connect import common as clickhouse_connect_common
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import (
    ClickHouseError,
    OperationalError,
    StreamFailureError,
)
from clickhouse_connect.driver.httputil import get_pool_manager
from sentry_sdk import traces

from snuba import environment, settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import (
    ClickhousePool,
    ClickhouseProfile,
    ClickhouseResult,
    Params,
)
from snuba.reader import unwrap_nullable_type
from snuba.state.sentry_options import get_option
from snuba.utils.concurrency import process_query_concurrency
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.sentry import SENTRY_OP

metrics = MetricsWrapper(environment.metrics, "clickhouse.connect")


def _driver_params(params: Params) -> Sequence[Any] | dict[str, Any] | None:
    """Narrow ``Params`` to clickhouse-connect's accepted forms.

    Falsy => None. Mappings are copied to a plain ``dict`` (driver wants an
    invariant dict); sequences pass through as positional binds.
    """
    if not params:
        return None
    if isinstance(params, Mapping):
        return dict(params)
    return params


def _insert_statement(table: str, column_names: Sequence[str]) -> str:
    """Rebuild the INSERT statement clickhouse-connect sends on the wire.

    ``client.insert`` takes a row matrix, not SQL, but still emits
    ``INSERT INTO <table> (<cols>) FORMAT Native``. Row data is excluded
    (unbounded, may hold PII).
    """
    columns = ", ".join(quote_identifier(name) for name in column_names)
    return f"INSERT INTO {table} ({columns}) FORMAT Native"


# Stand-in for "no read timeout" on the HTTP path. The native driver maps a
# profile with no timeout (``None``) to an unbounded socket, but clickhouse-connect
# cannot safely take ``None`` (its progress-interval computation does arithmetic
# on the value and would fail), so we pass a very large finite timeout instead —
# effectively unbounded for any real operation. Per-profile timeouts that are set
# (e.g. 25s for reads, longer for migrations) are honored as-is.
UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS = 86_400  # 24h

# Default ClickHouse HTTP port, used when a caller does not pass one.
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123

# The clickhouse_connect_pool_size schema default, from when pool sizes were a
# flat constant. Pool size now derives from the process's query concurrency, but
# a live option's default cannot be changed, so this value means "not set".
LEGACY_CONNECT_POOL_SIZE = 25

# Match native-driver behavior: forward unknown settings instead of failing.
clickhouse_connect_common.set_setting("invalid_setting_action", "drop")
# Process-wide; default off (plain Native framing).
clickhouse_connect_common.set_setting(
    "use_protocol_version",
    get_option("clickhouse_connect_use_protocol_version", False),
)

_STREAM_DESYNC_MARKERS = (
    "Unrecognized ClickHouse type",
    "Stream ended unexpectedly",
    "Stream failed during read",
    "unrecognized data found in stream",
)


def _coerce_temporal(value: Any, ch_type: str) -> Any:
    """
    Parse a ``Date``/``DateTime`` string from JSONCompact into the ``date``/``datetime``
    object the reader's transforms expect -- they call date/datetime methods on it and
    would crash on a raw string. Other types (and non-strings) pass through unchanged.
    """
    if not isinstance(value, str):
        return value
    _, inner = unwrap_nullable_type(ch_type)
    if not inner.startswith("Date"):  # Date, Date32, DateTime, DateTime64
        return value
    parsed = datetime.fromisoformat(value)
    # DateTime/DateTime64 keep the time; Date/Date32 want a bare date.
    return parsed if inner.startswith("DateTime") else parsed.date()


class ClickhouseConnectPool(ClickhousePool):
    """
    HTTP based ClickHouse client backed by ``clickhouse-connect``.

    It subclasses :class:`snuba.clickhouse.native.ClickhousePool` and overrides
    the ``execute`` / ``execute_robust`` / ``close`` interface so it is a true
    drop-in replacement. The decision of which pool to instantiate is made by
    the connection cache (see :mod:`snuba.clusters.cluster`), one level above
    the individual drivers.

    Unlike the native pool, this class does not maintain its own queue of
    connections: ``clickhouse-connect`` manages an HTTP connection pool (via
    ``urllib3``) for us. A single :class:`Client` is created lazily and reused
    across threads, with the underlying pool sized to ``max_pool_size``.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        http_port: int = DEFAULT_CLICKHOUSE_HTTP_PORT,
        secure: bool = False,
        ca_certs: str | None = None,
        verify: bool | None = False,
        connect_timeout: int = 1,
        send_receive_timeout: int | None = 35,
        client_settings: Mapping[str, Any] = {},
    ) -> None:
        self.host = host
        self.port = http_port
        self.user = user
        self.password = password
        self.database = database
        self.secure = secure
        self.ca_certs = ca_certs
        self.verify = verify
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.client_settings = client_settings

        self.__client: Client | None = None
        self.__lock = Lock()

    def _create_client(self) -> Client:
        # The option keeps its historical default, which schema evolution will not
        # let us change, so that value means "unset" and defers to the derived
        # size. Any other positive value is an explicit operator override.
        option_pool_size = get_option("clickhouse_connect_pool_size", LEGACY_CONNECT_POOL_SIZE)
        if option_pool_size > 0 and option_pool_size != LEGACY_CONNECT_POOL_SIZE:
            pool_size = option_pool_size
        else:
            pool_size = settings.CLICKHOUSE_MAX_POOL_SIZE or process_query_concurrency()
        pool_mgr = get_pool_manager(
            ca_cert=self.ca_certs,
            verify=bool(self.verify),
            maxsize=pool_size,
            num_pools=1,
        )
        connect_timeout = (
            get_option("clickhouse_connect_connect_timeout", 0) or self.connect_timeout
        )
        send_receive_timeout = get_option("clickhouse_connect_send_receive_timeout", 0)
        if not send_receive_timeout:
            send_receive_timeout = (
                self.send_receive_timeout
                if self.send_receive_timeout is not None
                else UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS
            )
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
            interface="https" if self.secure else "http",
            secure=self.secure,
            verify=bool(self.verify),
            ca_cert=self.ca_certs,
            connect_timeout=connect_timeout,
            send_receive_timeout=send_receive_timeout,
            settings=dict(self.client_settings),
            pool_mgr=pool_mgr,
            query_limit=0,
            autogenerate_session_id=False,
            compress="lz4",
        )

    def _get_client(self) -> Client:
        if self.__client is None:
            with self.__lock:
                if self.__client is None:
                    self.__client = self._create_client()
        return self.__client

    def _reset_connections(self) -> None:
        if self.__client is not None:
            with suppress(Exception):
                self.__client.close_connections()

    def _build_query_settings(
        self,
        settings: Mapping[str, Any] | None,
        query_id: str | None,
        capture_trace: bool,
    ) -> dict[str, Any] | None:
        query_settings: dict[str, Any] = dict(settings) if settings else {}
        if query_id is not None:
            query_settings["query_id"] = query_id
        if capture_trace:
            # We still ask the server to emit trace logs, but unlike the native
            # driver clickhouse-connect does not surface them (it only reads the
            # X-ClickHouse-Summary header), so ``trace_output`` ends up empty on
            # this path. See the note in _execute_once. Practically this means
            # the snuba-admin trace view and its profile-events parsing return
            # nothing when the HTTP driver is enabled; every other admin query
            # path is driver-agnostic. Reconstructing traces over HTTP would
            # require querying system.text_log by query_id (a separate feature).
            query_settings["send_logs_level"] = "trace"
        return query_settings or None

    @staticmethod
    def _is_stream_desync(exc: BaseException) -> bool:
        message = str(exc)
        return any(marker in message for marker in _STREAM_DESYNC_MARKERS)

    def _execute_once(
        self,
        query: str,
        params: Params,
        with_column_types: bool,
        query_id: str | None,
        settings: Mapping[str, Any] | None,
        columnar: bool,
        capture_trace: bool,
    ) -> ClickhouseResult:
        client = self._get_client()
        query_settings = self._build_query_settings(settings, query_id, capture_trace)

        with traces.start_span(
            name="clickhouse query",
            attributes={
                SENTRY_OP: "db.clickhouse",
                sentry_sdk.consts.SPANDATA.DB_SYSTEM: "clickhouse",
                sentry_sdk.consts.SPANDATA.DB_QUERY_TEXT: query,
            },
        ) as span:
            if query_id is not None:
                span.set_attribute("query_id", query_id)
            span.set_attribute("settings", json.dumps(query_settings, default=repr))
            query_result = client.query(
                query,
                parameters=_driver_params(params),
                settings=query_settings,
                column_oriented=columnar,
            )

        summary = query_result.summary or {}

        def _int(key: str) -> int:
            value = summary.get(key)
            try:
                return int(value) if value is not None else 0
            except (TypeError, ValueError):
                return 0

        elapsed_ns = summary.get("elapsed_ns")
        try:
            elapsed = float(elapsed_ns) / 1e9 if elapsed_ns is not None else 0.0
        except (TypeError, ValueError):
            elapsed = 0.0

        profile_data = ClickhouseProfile(
            blocks=0,
            bytes=_int("read_bytes"),
            elapsed=elapsed,
            progress_bytes=_int("read_bytes"),
            rows=_int("read_rows"),
        )

        results: Sequence[Any] = query_result.result_set

        # trace_output is always empty here: clickhouse-connect has no mechanism
        # for capturing the server's send_logs_level output (it only parses the
        # X-ClickHouse-Summary header for the profile above). This is a known,
        # accepted limitation of the HTTP path — see _build_query_settings.
        if not with_column_types:
            return ClickhouseResult(
                results=results,
                profile=profile_data,
                trace_output="",
            )

        meta: list[tuple[str, str]] = [
            (name, column_type.name)
            for name, column_type in zip(
                query_result.column_names, query_result.column_types, strict=True
            )
        ]

        return ClickhouseResult(
            results=results,
            meta=meta,
            profile=profile_data,
            trace_output="",
        )

    def execute_with_totals(
        self,
        query: str,
        params: Params = None,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        capture_trace: bool = False,
        robust: bool = False,
    ) -> ClickhouseResult:
        """
        HTTP override of :meth:`ClickhousePool.execute_with_totals`. clickhouse-connect's
        Native/HTTP output drops the ``WITH TOTALS`` row, so run the query once with
        ``FORMAT JSONCompact`` (data + meta + totals together) and append the totals as
        the trailing result row, the shape the reader expects. ``capture_trace``/``robust``
        are accepted for interface parity but unused on this path.
        """
        with self._translate_clickhouse_errors():
            client = self._get_client()
            json_settings: dict[str, Any] = dict(settings) if settings else {}
            # 64-bit ints as JSON numbers, matching the native driver's Python ints.
            json_settings["output_format_json_quote_64bit_integers"] = 0
            if query_id is not None:
                json_settings["query_id"] = query_id

            with traces.start_span(
                name="clickhouse query",
                attributes={
                    SENTRY_OP: "db.clickhouse",
                    sentry_sdk.consts.SPANDATA.DB_SYSTEM: "clickhouse",
                    sentry_sdk.consts.SPANDATA.DB_QUERY_TEXT: query,
                },
            ) as span:
                if query_id is not None:
                    span.set_attribute("query_id", query_id)
                raw = client.raw_query(
                    query,
                    parameters=_driver_params(params),
                    settings=json_settings,
                    fmt="JSONCompact",
                )

            payload = json.loads(raw)
            meta = [(column["name"], column["type"]) for column in payload.get("meta", [])]
            column_types = [ch_type for _, ch_type in meta]

            # Each data row and the totals row is a positional array aligned to meta.
            results: list[tuple[Any, ...]] = [
                tuple(
                    _coerce_temporal(value, column_types[index]) for index, value in enumerate(row)
                )
                for row in payload.get("data", [])
            ]
            totals = payload.get("totals")
            if totals:
                results.append(
                    tuple(
                        _coerce_temporal(value, column_types[index])
                        for index, value in enumerate(totals)
                    )
                )

            return ClickhouseResult(
                results=results,
                meta=meta,
                profile=self._profile_from_statistics(payload),
                trace_output="",
            )

    @staticmethod
    def _profile_from_statistics(payload: Mapping[str, Any]) -> ClickhouseProfile:
        # JSON's ``statistics`` object carries the same read counters as the Native
        # path's X-ClickHouse-Summary header.
        statistics = payload.get("statistics") or {}

        def _int(key: str) -> int:
            try:
                return int(statistics.get(key) or 0)
            except (TypeError, ValueError):
                return 0

        try:
            elapsed = float(statistics.get("elapsed") or 0.0)
        except (TypeError, ValueError):
            elapsed = 0.0

        read_bytes = _int("bytes_read")
        return ClickhouseProfile(
            blocks=0,
            bytes=read_bytes,
            elapsed=elapsed,
            progress_bytes=read_bytes,
            rows=_int("rows_read"),
        )

    @contextmanager
    def _translate_clickhouse_errors(self) -> Iterator[None]:
        try:
            yield
        except OperationalError as e:
            metrics.increment("connection_error")
            self._reset_connections()
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except StreamFailureError as e:
            metrics.increment("stream_failure")
            self._reset_connections()
            raise ClickhouseError(str(e), code=-1) from e
        except ClickHouseError as e:
            if self._is_stream_desync(e):
                metrics.increment("stream_desync")
                self._reset_connections()
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except json.JSONDecodeError as e:
            raise ClickhouseError(f"invalid JSON response: {e}", code=-1) from e

    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """Execute a clickhouse query. ``retryable`` is accepted for interface parity only."""
        with self._translate_clickhouse_errors():
            return self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
            )

    def insert(
        self,
        table: str,
        data: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any] | None = None,
        query_id: str | None = None,
    ) -> None:
        rows = list(data)
        if not rows:
            return

        column_names = list(rows[0].keys())
        matrix = [list(row.values()) for row in rows]

        insert_settings = dict(settings) if settings else {}
        if query_id is not None:
            insert_settings["query_id"] = query_id

        with self._translate_clickhouse_errors():
            client = self._get_client()
            with traces.start_span(
                name=f"INSERT INTO {table}",
                attributes={
                    SENTRY_OP: "db.clickhouse",
                    sentry_sdk.consts.SPANDATA.DB_SYSTEM: "clickhouse",
                    sentry_sdk.consts.SPANDATA.DB_QUERY_TEXT: _insert_statement(
                        table, column_names
                    ),
                },
            ) as span:
                span.set_attribute(
                    "query_id",
                    query_id if query_id is not None else "unknown-query-id",
                )
                client.insert(
                    table,
                    matrix,
                    column_names=column_names,
                    settings=insert_settings or None,
                )

    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """
        Mirrors :meth:`ClickhousePool.execute_robust`. Since retries are
        delegated to clickhouse-connect, this is equivalent to :meth:`execute`.
        """
        return self.execute(
            query,
            params=params,
            with_column_types=with_column_types,
            query_id=query_id,
            settings=settings,
            types_check=types_check,
            columnar=columnar,
            capture_trace=capture_trace,
            retryable=retryable,
        )

    def execute_explain(self, query: str) -> ClickhouseResult:
        """
        Run an EXPLAIN statement over HTTP and return its single ``explain`` text
        column, one row per line. Overrides :meth:`ClickhousePool.execute_explain`.

        EXPLAIN needs its own path on this driver. ``query()`` appends
        ``FORMAT Native`` and decodes the response with its binary Native reader;
        for an EXPLAIN that trailing format is consumed by the *inner* query being
        explained, so the EXPLAIN's own output comes back as text and the Native
        reader misfires — the cryptic ``Unrecognized ClickHouse type ...`` error
        (a fragment of the explain dump read as a column type). ``command()``
        instead sends the statement verbatim — no FORMAT appended — and returns
        the decoded text, which we split into one single-column row per line, the
        same shape the native driver returns for the same EXPLAIN.

        This serves the single-column explain output of EXPLAIN AST / QUERY TREE /
        SYNTAX / PLAN / PIPELINE (the kinds admin system-query validation issues);
        the multi-column EXPLAIN ESTIMATE is not used on this path.
        """
        with self._translate_clickhouse_errors():
            client = self._get_client()
            with traces.start_span(
                name="clickhouse query",
                attributes={
                    SENTRY_OP: "db.clickhouse",
                    sentry_sdk.consts.SPANDATA.DB_SYSTEM: "clickhouse",
                    sentry_sdk.consts.SPANDATA.DB_QUERY_TEXT: query,
                },
            ):
                output = client.command(query)
            return self._explain_result(output)

    @staticmethod
    def _explain_result(output: object) -> ClickhouseResult:
        # command() returns the decoded body: a str for our single-column,
        # tab-free explain output (it has already stripped the trailing newline).
        # Normalize the other documented return shapes defensively before
        # splitting into one row per line.
        if isinstance(output, str):
            text = output
        elif isinstance(output, int):
            text = str(output)
        elif isinstance(output, (list, tuple)):
            # command() only returns a sequence when the body contained tab
            # characters; explain output is space-indented and tab-free, so this
            # is defensive. Re-join so the per-line split preserves the layout.
            text = "\t".join(str(part) for part in output)
        else:
            # QuerySummary (empty body) or anything unexpected -> no rows.
            text = ""

        results: list[tuple[str, ...]] = [(line,) for line in text.split("\n")] if text else []
        profile = ClickhouseProfile(
            bytes=0, progress_bytes=0, blocks=0, rows=len(results), elapsed=0.0
        )
        return ClickhouseResult(
            results=results, meta=[("explain", "String")], profile=profile, trace_output=""
        )

    def close(self) -> None:
        # Take the same lock _get_client uses so a concurrent lazy init can't
        # race with teardown (one thread closing the client while another is
        # creating or about to use it).
        with self.__lock:
            if self.__client is not None:
                self.__client.close()
                self.__client = None
