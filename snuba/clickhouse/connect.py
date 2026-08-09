from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import datetime
from threading import Lock, local
from typing import Any, NamedTuple

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
from clickhouse_connect.driver.httputil import all_managers, get_pool_manager
from sentry_sdk import traces
from urllib3.poolmanager import PoolManager

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

# urllib3 keeps one connection pool per host; this bounds how many it retains
# before evicting the least-recently-used one. A process talks to the nodes of a
# handful of clusters, so this is slack, not a budget.
_MAX_ENDPOINTS_PER_PROCESS = 64

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


class ClientKey(NamedTuple):
    """What makes two clients interchangeable.

    Everything that is baked into a :class:`Client` at construction: where it
    connects, who it connects as, and the socket timeout it reads with. Notably
    *not* the profile's query settings -- those vary per request and are applied
    per query instead (see ``_build_query_settings``), which is what lets one
    client serve every profile that shares a timeout.
    """

    host: str
    port: int
    database: str
    user: str
    password: str
    secure: bool
    ca_certs: str | None
    verify: bool
    connect_timeout: int
    send_receive_timeout: int


class ClickhouseClientManager:
    """Owner of the ClickHouse HTTP clients and the sockets behind them.

    Owned by :class:`~snuba.clusters.cluster.ConnectionCache`, which builds one
    on first use of the connect driver and hands it to every pool it creates.
    That is what makes it process-wide: the cache is the process-wide owner of
    connections, and this is the part of it that knows about HTTP. It is not a
    singleton of its own, and pools are given one rather than reaching for a
    global, so tests get a real manager without touching shared state.

    It owns the urllib3 pool manager -- the socket pool -- and hands out a
    :class:`Client` per :class:`ClientKey`, **per thread**.

    Clients are thread-local rather than shared, so a client only ever has one
    query in flight: a granian blocking thread serves one request at a time, so
    queries are serialized per thread by construction and nothing has to
    coordinate access to a client's response stream. That is cheap because a
    client owns no sockets of its own -- every client draws from the one shared
    pool manager, which urllib3 multiplexes per host. The socket ceiling is
    therefore the query concurrency per endpoint, not the number of clients.

    Sockets are still only reusable because every request drains its response
    before returning (see ``ClickhouseConnectPool._execute_once``); a half-read
    socket going back to the pool is what makes the next request decode another
    request's bytes.

    A separate pool manager is kept per TLS configuration, since urllib3 fixes
    cert verification at manager level. Deployments normally have exactly one.
    """

    def __init__(self) -> None:
        self.__lock = Lock()
        self.__local = local()
        self.__pool_managers: dict[tuple[str | None, bool], PoolManager] = {}

    def _pool_manager(self, ca_certs: str | None, verify: bool) -> PoolManager:
        key = (ca_certs, verify)
        manager = self.__pool_managers.get(key)
        if manager is not None:
            return manager
        with self.__lock:
            # Re-check under the lock. Clients are thread-local and need no
            # locking, but this map is shared, and losing this race is not
            # benign: two managers for one key means two socket pools, each with
            # its own maxsize, so the process-wide ceiling stops holding. The
            # loser is also unreachable for close() while still pinned in
            # clickhouse-connect's all_managers, and the thread that built it
            # keeps using it for as long as it lives.
            manager = self.__pool_managers.get(key)
            if manager is None:
                manager = get_pool_manager(
                    ca_cert=ca_certs,
                    verify=verify,
                    # Per host, and a granian blocking thread runs one query at
                    # a time, so this is the most sockets one endpoint can need.
                    maxsize=_resolve_pool_size(),
                    num_pools=_MAX_ENDPOINTS_PER_PROCESS,
                )
                self.__pool_managers[key] = manager
            return manager

    def get_client(self, key: ClientKey) -> Client:
        # Thread-local, so no locking and no chance of two threads landing on
        # one client. The dict dies with the thread; the sockets do not, because
        # they belong to the shared pool manager below.
        clients: dict[ClientKey, Client] | None = getattr(self.__local, "clients", None)
        if clients is None:
            clients = self.__local.clients = {}
        client = clients.get(key)
        if client is None:
            client = clients[key] = self._build_client(key)
        return client

    def _build_client(self, key: ClientKey) -> Client:
        return clickhouse_connect.get_client(
            host=key.host,
            port=key.port,
            username=key.user,
            password=key.password,
            database=key.database,
            interface="https" if key.secure else "http",
            secure=key.secure,
            verify=key.verify,
            ca_cert=key.ca_certs,
            connect_timeout=key.connect_timeout,
            send_receive_timeout=key.send_receive_timeout,
            # No `settings=`: profile settings are per query, not per client.
            pool_mgr=self._pool_manager(key.ca_certs, key.verify),
            query_limit=0,
            # A client is used by one thread, one query at a time, so let the
            # driver stamp the query id when the caller has not supplied one.
            # That makes every request identifiable client-side -- including the
            # insert, explain and migration paths, which pass no id -- instead
            # of only being findable by whatever ClickHouse assigned it.
            autogenerate_query_id=True,
            # Server-side sessions would serialize per thread too, but they are
            # ClickHouse state with their own limits (max_sessions_for_user);
            # thread-local clients already give the serialization without it.
            autogenerate_session_id=False,
            compress="lz4",
        )

    def reset_after_fork(self) -> None:
        """Drop everything the child inherited from the parent.

        The child must not use the parent's sockets, and must not close them
        either -- the descriptors are shared, so closing here would be reaching
        into the parent's connections. Dropping the references is enough; the
        child rebuilds on next use.

        Popping the managers out of clickhouse-connect's ``all_managers`` is the
        part that is easy to miss: that registry is process-global and is
        inherited too, so clearing only our own map would leave the parent's
        managers pinned in the child for the life of the process.

        The lock is rebuilt as well: a lock held by another thread at fork time
        is inherited held, and no thread exists in the child to release it.
        """
        for manager in self.__pool_managers.values():
            all_managers.pop(manager, None)
        self.__pool_managers = {}
        self.__local = local()
        self.__lock = Lock()

    def close(self) -> None:
        """Close every socket this process holds.

        Clearing the pool managers is what actually releases resources: clients
        own no sockets, so a client left behind on another thread holds nothing
        and is collected with that thread. ``Client.close`` would not help here
        anyway -- it only tears down a pool manager the client created itself,
        and we hand ours in, which also leaves them pinned in
        clickhouse-connect's module-level ``all_managers``.
        """
        with self.__lock:
            for manager in self.__pool_managers.values():
                with suppress(Exception):
                    manager.clear()
                all_managers.pop(manager, None)
            self.__pool_managers.clear()
        self.__local = local()


def _resolve_pool_size() -> int:
    """How many sockets urllib3 may hold open per ClickHouse endpoint."""
    # The option keeps its historical default, which schema evolution will not
    # let us change, so that value means "unset" and defers to the derived
    # size. Any other positive value is an explicit operator override.
    option_pool_size = get_option("clickhouse_connect_pool_size", LEGACY_CONNECT_POOL_SIZE)
    if option_pool_size > 0 and option_pool_size != LEGACY_CONNECT_POOL_SIZE:
        return int(option_pool_size)
    return settings.CLICKHOUSE_MAX_POOL_SIZE or process_query_concurrency()


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

    This class is the per-profile face of a shared resource. It holds no client
    and no socket of its own: it knows *where* to connect and *with which
    settings*, and asks the :class:`ClickhouseClientManager` it was given for
    this thread's client for its endpoint and socket timeout. That manager
    belongs to the :class:`~snuba.clusters.cluster.ConnectionCache` that built
    this pool. Many pools -- one per profile per node --
    therefore collapse onto a handful of clients per thread, and every thread
    draws from one process-wide socket pool.

    Two rules make that sharing safe, and both live here:

    * The profile's ClickHouse settings are applied **per query**, never baked
      into the client, so profiles that share a timeout (QUERY and TRACING) do
      not leak settings into each other.
    * Every request **drains its response** before returning, so the socket
      going back to urllib3 has nothing left on it. A half-read socket returned
      to the pool is precisely how one request ends up decoding the tail of
      another request's response.
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
        *,
        client_manager: ClickhouseClientManager,
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
        # The profile's ClickHouse settings. Applied per query rather than baked
        # into the client, so profiles that differ only in settings (QUERY and
        # TRACING, say, which share a 25s timeout) can share one client without
        # TRACING's `readonly` leaking onto QUERY's queries.
        self.client_settings = client_settings
        self.__client_manager = client_manager

        # Resolved once, here, because the timeouts are part of the client key:
        # reading the options per request would mint a new key -- and so a new
        # cached client -- every time an operator changed one, and nothing ever
        # evicts the old one. Both options are documented as "read when a client
        # is created", so resolving at construction is also what they promise.
        # Pools are cached for the process lifetime, so a change takes effect on
        # restart, as it did before.
        self.__connect_timeout = (
            get_option("clickhouse_connect_connect_timeout", 0) or connect_timeout
        )
        resolved_send_receive = get_option("clickhouse_connect_send_receive_timeout", 0)
        if not resolved_send_receive:
            resolved_send_receive = (
                send_receive_timeout
                if send_receive_timeout is not None
                else UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS
            )
        self.__send_receive_timeout = resolved_send_receive

    def _client_key(self) -> ClientKey:
        return ClientKey(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            secure=self.secure,
            ca_certs=self.ca_certs,
            verify=bool(self.verify),
            connect_timeout=self.__connect_timeout,
            send_receive_timeout=self.__send_receive_timeout,
        )

    def _get_client(self) -> Client:
        return self.__client_manager.get_client(self._client_key())

    def _build_query_settings(
        self,
        settings: Mapping[str, Any] | None,
        query_id: str | None,
        capture_trace: bool,
    ) -> dict[str, Any] | None:
        # Profile settings first so an explicit per-query setting still wins.
        query_settings: dict[str, Any] = dict(self.client_settings)
        if settings:
            query_settings.update(settings)
        if query_id is not None:
            query_settings["query_id"] = query_id
        if capture_trace:
            # clickhouse-connect does not surface send_logs_level output; tracing
            # recovers performance data from system.query_log instead.
            query_settings["send_logs_level"] = "trace"
        return query_settings or None

    @staticmethod
    def _is_stream_desync(exc: BaseException) -> bool:
        message = str(exc)
        return any(marker in message for marker in _STREAM_DESYNC_MARKERS)

    def _execute_once(
        self,
        client: Client,
        query: str,
        params: Params,
        with_column_types: bool,
        query_id: str | None,
        settings: Mapping[str, Any] | None,
        columnar: bool,
        capture_trace: bool,
    ) -> ClickhouseResult:
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

        try:
            return self._consume(query_result, with_column_types, query_id)
        finally:
            # The single most important line for a shared client. `query()`
            # returns a lazy result over a live socket; `QueryResult.close()`
            # drains whatever is left of the response and hands the connection
            # back clean. Reading `result_set` happens to do this today, via
            # StreamContext.__exit__, but only if nothing raises first and only
            # for as long as that stays true -- and a socket returned to the
            # pool with bytes still on it is exactly how one request ends up
            # decoding another request's response.
            with suppress(Exception):
                query_result.close()

    def _consume(
        self, query_result: Any, with_column_types: bool, query_id: str | None
    ) -> ClickhouseResult:
        """Turn a QueryResult into a ClickhouseResult. Never leaves the response open.

        Split out of ``_execute_once`` so the drain can sit in a ``finally``
        around it; ``query_id`` is threaded through because the server-reported
        id falls back to the one we sent.
        """
        summary = query_result.summary or {}
        result_query_id = str(query_result.query_id or query_id or "")

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

        if not with_column_types:
            return ClickhouseResult(
                results=results,
                profile=profile_data,
                trace_output="",
                query_id=result_query_id,
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
            query_id=result_query_id,
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
            # Profile settings first, exactly as on the execute() path: they
            # used to ride on the client and so applied to every operation.
            json_settings: dict[str, Any] = dict(self.client_settings)
            if settings:
                json_settings.update(settings)
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
        # No client disposal here. Clients are shared, and a request that
        # drains its response leaves the socket clean whatever the outcome; a
        # broken connection is urllib3's to drop, not ours to blanket-reset.
        try:
            yield
        except OperationalError as e:
            metrics.increment("connection_error")
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except StreamFailureError as e:
            metrics.increment("stream_failure")
            raise ClickhouseError(str(e), code=-1) from e
        except ClickHouseError as e:
            if self._is_stream_desync(e):
                metrics.increment("stream_desync")
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
            client = self._get_client()
            return self._execute_once(
                client,
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

        insert_settings: dict[str, Any] = dict(self.client_settings)
        if settings:
            insert_settings.update(settings)
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
                output = client.command(query, settings=dict(self.client_settings) or None)
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
        # Clients belong to the process-wide manager, not to this pool -- other
        # pools (other profiles against the same endpoint) share them. Tearing
        # them down is the manager's call; a single pool going away is not a
        # reason to close sockets others are using.
        pass
