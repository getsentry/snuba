from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from threading import Lock
from typing import Any, TypeGuard

import clickhouse_connect
import sentry_sdk
from clickhouse_connect import common as clickhouse_connect_common
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError
from clickhouse_connect.driver.httputil import get_pool_manager

from snuba import environment, settings, state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import (
    ClickhousePool,
    ClickhouseProfile,
    ClickhouseResult,
    Params,
)
from snuba.reader import unwrap_nullable_type
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.clickhouse.connect")

metrics = MetricsWrapper(environment.metrics, "clickhouse.connect")

# Stand-in for "no read timeout" on the HTTP path. The native driver maps a
# profile with no timeout (``None``) to an unbounded socket, but clickhouse-connect
# cannot safely take ``None`` (its progress-interval computation does arithmetic
# on the value and would fail), so we pass a very large finite timeout instead —
# effectively unbounded for any real operation. Per-profile timeouts that are set
# (e.g. 25s for reads, longer for migrations) are honored as-is.
UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS = 86_400  # 24h

# Default ClickHouse HTTP port, used when a caller does not pass one.
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123

# clickhouse-connect raises a ProgrammingError by default when it is asked to
# send a setting it considers unknown or readonly. The native driver simply
# forwards whatever settings it is given to the server, so to preserve parity
# we tell clickhouse-connect to drop unrecognized settings instead of failing.
clickhouse_connect_common.set_setting("invalid_setting_action", "drop")

# Matches the leading ``INSERT INTO <table> [(col, col, ...)]`` of an insert
# statement, capturing the target table and (when present) the explicit column
# list. The native driver treats an INSERT whose ``params`` is a sequence of
# rows as a data insert; clickhouse-connect's ``query()`` has no such notion, so
# the connect pool detects this shape and sends the rows as a JSONEachRow insert
# body instead (see ``_execute_insert``). Whatever trails the column list
# (``VALUES``, ``FORMAT JSONEachRow``, ...) is rebuilt as ``FORMAT JSONEachRow``,
# so we only need the table and (for positional rows) the column list.
_INSERT_RE = re.compile(
    r"^\s*INSERT\s+INTO\s+(?P<table>[^\s(]+)\s*(?:\((?P<columns>[^)]*)\))?",
    re.IGNORECASE,
)


def _is_row_data(params: Params) -> TypeGuard[Sequence[Any]]:
    """
    True when ``params`` is a sequence of rows (insert data) rather than a
    mapping of query-substitution parameters. ``str``/``bytes`` are sequences
    but never row data, so they are excluded. A ``Mapping`` is not a ``Sequence``
    and is therefore already excluded.
    """
    return isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray))


def _clickhouse_json_default(value: Any) -> Any:
    """
    ``json.dumps`` fallback that renders the Python types ClickHouse's JSONEachRow
    parser cannot take as bare JSON into the string forms it accepts. Only
    ``datetime``/``date`` need help here -- they are what the migration status
    writers put in a row, and the crash they caused ("Object of type datetime is
    not JSON serializable") is the whole reason this path exists.

    ``datetime`` is formatted to second precision (``YYYY-MM-DD HH:MM:SS``), the
    only form ClickHouse's default ``date_time_input_format=basic`` accepts for a
    ``DateTime`` column -- a fractional part would be rejected. ``date`` becomes
    ``YYYY-MM-DD``. ``datetime`` is checked first because it subclasses ``date``.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


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
        # No native connection queue here; clickhouse-connect manages its own
        # HTTP pool. ``port`` is the abstract base attribute (it holds the
        # cluster's configured HTTP port for this driver). The pool size is not
        # a construction parameter: it is always taken from the
        # ``clickhouse_connect_pool_size`` runtime config (see _get_client), so
        # it can be tuned at runtime without rebuilding pools.
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

    def _get_client(self) -> Client:
        # The client (and its handshake with the server) is created lazily so
        # that simply constructing a pool does not open a connection.
        if self.__client is None:
            with self.__lock:
                if self.__client is None:
                    # Pool size always comes from the clickhouse_connect_pool_size
                    # runtime config, falling back to the configured
                    # CLICKHOUSE_MAX_POOL_SIZE. The value is read once, when the
                    # (cached) client is first created.
                    pool_size = (
                        state.get_int_config(
                            "clickhouse_connect_pool_size", settings.CLICKHOUSE_MAX_POOL_SIZE
                        )
                        or settings.CLICKHOUSE_MAX_POOL_SIZE
                    )
                    pool_mgr = get_pool_manager(
                        ca_cert=self.ca_certs,
                        verify=bool(self.verify),
                        maxsize=pool_size,
                        # All requests go to a single host, so a single pool is
                        # enough. Keep a small margin for safety.
                        num_pools=2,
                    )
                    self.__client = clickhouse_connect.get_client(
                        host=self.host,
                        port=self.port,
                        username=self.user,
                        password=self.password,
                        database=self.database,
                        interface="https" if self.secure else "http",
                        secure=self.secure,
                        verify=bool(self.verify),
                        ca_cert=self.ca_certs,
                        connect_timeout=self.connect_timeout,
                        # Honor the per-profile timeout as-is, like the native
                        # driver does (reads get 25s, migrations/DDL keep their
                        # longer timeouts). A profile with no timeout means
                        # "unbounded" on the native path; emulate that here with a
                        # large finite timeout, since clickhouse-connect cannot
                        # take None.
                        send_receive_timeout=(
                            self.send_receive_timeout
                            if self.send_receive_timeout is not None
                            else UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS
                        ),
                        settings=dict(self.client_settings),
                        pool_mgr=pool_mgr,
                        # The native driver applies no implicit row limit; match
                        # that behavior here.
                        query_limit=0,
                        # Sessions serialize queries on the server. We share a
                        # single client across threads, so sessions must be
                        # disabled to allow concurrent queries.
                        autogenerate_session_id=False,
                    )
        return self.__client

    def _build_query_settings(
        self,
        settings: Mapping[str, Any] | None,
        query_id: str | None,
        capture_trace: bool,
    ) -> Mapping[str, Any] | None:
        query_settings = dict(settings) if settings else {}
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

        with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
            span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
            span.set_data("query_id", query_id)
            span.set_data("settings", query_settings)
            query_result = client.query(
                query,
                parameters=params if params else None,
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

            with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
                span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
                span.set_data("query_id", query_id)
                raw = client.raw_query(
                    query,
                    parameters=params if params else None,
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
        # Map clickhouse-connect's transport/server errors onto snuba's
        # ClickhouseError (preserving the server error code), mirroring how the
        # native pool wraps the clickhouse_driver error family. Shared by
        # execute() and execute_explain() so both surface failures identically.
        try:
            yield
        except OperationalError as e:
            # Connection/transport level failures. Mirrors the native pool's
            # handling of NetworkError/SocketTimeoutError by emitting the
            # connection_error metric before surfacing the error.
            metrics.increment(
                "connection_error",
                tags={
                    "host": self.host,
                    "port": str(self.port),
                    "user": self.user,
                    "database": self.database,
                },
            )
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except ClickHouseError as e:
            # ClickHouseError is the base class for every clickhouse-connect
            # error (DatabaseError, ProgrammingError, DataError, ...). The native
            # pool likewise wraps the whole clickhouse_driver errors.Error family
            # into ClickhouseError, preserving the server error code when present.
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except json.JSONDecodeError as e:
            # A malformed body on the JSONCompact totals path (truncation, a proxy
            # error page) surfaces as ClickhouseError, like the native driver does.
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
        """
        Execute a clickhouse query.

        Unlike :class:`snuba.clickhouse.native.ClickhouseNativePool`, this
        method does not implement any retry logic of its own. Retries (stale
        keep-alive sockets, transport errors and HTTP 429/503/504 responses)
        are handled internally by clickhouse-connect. Notably this means the
        native pool's ``TOO_MANY_SIMULTANEOUS_QUERIES`` backoff is *not*
        replicated: clickhouse-connect does not retry that error, so it is
        surfaced directly to the caller.

        The ``retryable`` argument is accepted for interface parity with the
        native pool but has no effect here.

        When ``params`` is a sequence of rows and ``query`` is an INSERT, the
        call is a data insert (the native driver's ``INSERT INTO ... FORMAT
        JSONEachRow`` + list-of-rows pattern). clickhouse-connect's ``query()``
        would treat those rows as substitution parameters and JSON-encode them,
        which fails for native Python values such as ``datetime``. Such calls are
        sent as a JSONEachRow insert body instead (see ``_execute_insert``).
        """
        with self._translate_clickhouse_errors():
            if params is not None and _is_row_data(params) and _INSERT_RE.match(query):
                return self._execute_insert(query, params, settings, query_id)
            return self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
            )

    def _execute_insert(
        self,
        query: str,
        rows: Sequence[Any],
        settings: Mapping[str, Any] | None,
        query_id: str | None,
    ) -> ClickhouseResult:
        """
        Insert a sequence of rows, reproducing the native driver's ``INSERT INTO
        ... + list-of-rows`` path over HTTP.

        The rows are serialized to a JSONEachRow body -- the same format the
        callers already name in their SQL -- and sent via
        ``client.raw_insert(..., fmt="JSONEachRow")``. Serialization uses
        :func:`_clickhouse_json_default` so native Python values that plain JSON
        rejects (notably ``datetime``, which triggered this whole path) are
        encoded into the string forms ClickHouse's JSONEachRow parser accepts.

        Rows may be dicts (as the migration status writers pass, mapping column
        name -> value) or positional sequences, in which case the column list is
        taken from the SQL (e.g. ``INSERT INTO t (a, b) VALUES``) and zipped with
        each row to form the JSON objects.
        """
        client = self._get_client()

        match = _INSERT_RE.match(query)
        if match is None:  # pragma: no cover - guarded by the caller
            raise ClickhouseError(f"could not parse INSERT target from query: {query}", code=-1)
        table = match.group("table")

        row_list = list(rows)
        if not row_list:
            # Nothing to insert; mirror the native driver, which writes zero rows.
            return ClickhouseResult(
                results=[],
                profile=ClickhouseProfile(blocks=0, bytes=0, elapsed=0.0, progress_bytes=0, rows=0),
                trace_output="",
            )

        if isinstance(row_list[0], Mapping):
            dict_rows: list[Mapping[str, Any]] = list(row_list)
        else:
            # positional rows need the column list from the SQL to become named
            # JSON objects; without it JSONEachRow has no field names to map to.
            columns_sql = match.group("columns")
            if not columns_sql:
                raise ClickhouseError(
                    "positional INSERT rows require an explicit column list in the query, "
                    f"e.g. 'INSERT INTO t (a, b) VALUES'; got: {query}",
                    code=-1,
                )
            column_names = [name.strip().strip("`") for name in columns_sql.split(",")]
            dict_rows = [dict(zip(column_names, row, strict=True)) for row in row_list]

        body = "\n".join(
            json.dumps(row, default=_clickhouse_json_default, separators=(",", ":"))
            for row in dict_rows
        )

        insert_settings = dict(settings) if settings else {}
        if query_id is not None:
            insert_settings["query_id"] = query_id

        with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
            span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
            span.set_data("query_id", query_id)
            summary = client.raw_insert(
                table,
                insert_block=body.encode("utf-8"),
                fmt="JSONEachRow",
                settings=insert_settings or None,
            )

        written_rows = getattr(summary, "written_rows", 0) or 0
        written_bytes = getattr(summary, "written_bytes", 0) or 0
        profile = ClickhouseProfile(
            blocks=0,
            bytes=written_bytes,
            elapsed=0.0,
            progress_bytes=written_bytes,
            rows=written_rows,
        )
        # The native driver returns the written row count for an INSERT (wrapped
        # in a list by execute()); mirror that shape. Both current callers ignore
        # the value beyond logging it.
        return ClickhouseResult(results=[written_rows], profile=profile, trace_output="")

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
            with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
                span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
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
