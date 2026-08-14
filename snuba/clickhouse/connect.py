from __future__ import annotations

import atexit
import json
import logging
import os
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
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
from clickhouse_connect.driver.httputil import all_managers, get_pool_manager
from clickhouse_connect.driver.query import limit_re, select_re
from sentry_sdk import traces
from urllib3.poolmanager import PoolManager

from snuba import environment, settings
from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import (
    ClickhousePool,
    ClickhouseProfile,
    ClickhouseResult,
    Params,
)
from snuba.reader import unwrap_nullable_type
from snuba.state.sentry_options import get_option
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.sentry import SENTRY_OP

logger = logging.getLogger("snuba.clickhouse")
metrics = MetricsWrapper(environment.metrics, "clickhouse.connect")

# Transport failures that used to be NetworkError / SocketTimeoutError / EOFError
# on the native driver. StreamFailureError and bare OperationalError map to -1.
_RETRYABLE_TRANSPORT_CODES = {
    -1,
    ErrorCodes.NETWORK_ERROR,
    ErrorCodes.SOCKET_TIMEOUT,
}

DEFAULT_SEND_RECEIVE_TIMEOUT_SECONDS = 60 * 60  # 1h fallback when profile timeout is None
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
_CLICKHOUSE_CONNECT_TRANSPORT_SETTINGS = {"X-ClickHouse-Format": "Native"}

clickhouse_connect_common.set_setting("invalid_setting_action", "drop")
clickhouse_connect_common.set_setting(
    "use_protocol_version",
    get_option("clickhouse_connect_use_protocol_version", False),
)

_pool_lock = Lock()
_pool_managers: dict[tuple[str | None, bool], PoolManager] = {}


def _shared_pool(ca_certs: str | None, verify: bool) -> PoolManager:
    key = (ca_certs, verify)
    manager = _pool_managers.get(key)
    if manager is not None:
        return manager
    with _pool_lock:
        manager = _pool_managers.get(key)
        if manager is None:
            manager = get_pool_manager(
                ca_cert=ca_certs,
                verify=verify,
                maxsize=get_option(
                    "clickhouse_connect_pool_size", settings.CLICKHOUSE_MAX_POOL_SIZE
                ),
                num_pools=16,
            )
            _pool_managers[key] = manager
        return manager


def _reset_pools_after_fork() -> None:
    """Drop inherited pool refs in the child without closing parent FDs."""
    global _pool_lock, _pool_managers
    for manager in _pool_managers.values():
        all_managers.pop(manager, None)
    _pool_managers = {}
    _pool_lock = Lock()


def _close_pools() -> None:
    """Release sockets held by the process-wide pool managers."""
    global _pool_managers
    with _pool_lock:
        for manager in _pool_managers.values():
            with suppress(Exception):
                manager.clear()
            all_managers.pop(manager, None)
        _pool_managers = {}


os.register_at_fork(after_in_child=_reset_pools_after_fork)
atexit.register(_close_pools)


def _driver_params(params: Params) -> Sequence[Any] | dict[str, Any] | None:
    if not params:
        return None
    if isinstance(params, Mapping):
        return dict(params)
    return params


def _insert_statement(table: str, column_names: Sequence[str]) -> str:
    columns = ", ".join(quote_identifier(name) for name in column_names)
    return f"INSERT INTO {table} ({columns}) FORMAT Native"


def _as_int(value: Any) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _coerce_temporal(value: Any, ch_type: str) -> Any:
    if not isinstance(value, str):
        return value
    _, inner = unwrap_nullable_type(ch_type)
    if not inner.startswith("Date"):
        return value
    parsed = datetime.fromisoformat(value)
    return parsed if inner.startswith("DateTime") else parsed.date()


def _apply_client_query_limit(client: Client, query: str) -> str:
    """Mirror ``Client._prep_query``: append the client's default LIMIT.

    ``raw_query`` does not apply ``query_limit`` (only ``query()`` does). Typed
    reads go through JSONCompact / ``raw_query``, so do it here. Skip when the
    SQL already has LIMIT or still contains SETTINGS (LIMIT after SETTINGS is
    invalid ClickHouse).
    """
    query_limit = getattr(client, "query_limit", 0)
    if not isinstance(query_limit, int) or query_limit <= 0:
        return query
    if select_re.search(query) is None or limit_re.search(query) is not None:
        return query
    if re.search(r"\bSETTINGS\b", query, re.IGNORECASE):
        return query
    return f"{query}\n LIMIT {query_limit}"


@contextmanager
def _query_span(
    sql: str, query_id: str | None = None, name: str = "clickhouse query"
) -> Iterator[Any]:
    with traces.start_span(
        name=name,
        attributes={
            SENTRY_OP: "db.clickhouse",
            sentry_sdk.consts.SPANDATA.DB_SYSTEM: "clickhouse",
            sentry_sdk.consts.SPANDATA.DB_QUERY_TEXT: sql,
        },
    ) as span:
        if query_id is not None:
            span.set_attribute("query_id", query_id)
        yield span


class ClickhouseConnectPool(ClickhousePool):
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        http_port: int = DEFAULT_CLICKHOUSE_HTTP_PORT,
        # TCP port is kept for node identity (system.clusters, migration
        # state keys). Connections use http_port.
        tcp_port: int | None = None,
        secure: bool = False,
        ca_certs: str | None = None,
        verify: bool | None = False,
        connect_timeout: int = 1,
        send_receive_timeout: int | None = 35,
        client_settings: Mapping[str, Any] = {},
        # clickhouse-connect client-side default LIMIT for SELECTs with no LIMIT.
        # 0 disables. Kept off by default so prod query paths stay unbounded.
        query_limit: int = 0,
    ) -> None:
        self.host = host
        # Callers still key nodes by the TCP port (e.g. get_column_states).
        self.port = tcp_port if tcp_port is not None else http_port
        self.http_port = http_port
        self.user = user
        self.password = password
        self.database = database
        self.secure = secure
        self.ca_certs = ca_certs
        self.verify = verify
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.client_settings = client_settings
        self.query_limit = query_limit

    def _new_client(self, use_database: bool = True, query_limit: int | None = None) -> Client:
        connect_timeout = (
            get_option("clickhouse_connect_connect_timeout", 0) or self.connect_timeout
        )
        send_receive_timeout = get_option("clickhouse_connect_send_receive_timeout", 0)
        if not send_receive_timeout:
            send_receive_timeout = (
                self.send_receive_timeout
                if self.send_receive_timeout is not None
                else DEFAULT_SEND_RECEIVE_TIMEOUT_SECONDS
            )
        with traces.start_span(
            name="clickhouse client",
            attributes={
                SENTRY_OP: "db.clickhouse",
                sentry_sdk.consts.SPANDATA.DB_SYSTEM: "clickhouse",
                "server.address": self.host,
                "server.port": str(self.http_port),
            },
        ):
            # Do not pass ``database`` into get_client. clickhouse-connect's
            # autoconnect probes ``system.settings`` with the client database
            # attached; if that database is missing (test bootstrap creates
            # ``snuba_test`` via a pool opened on ``default``, migrations that
            # recreate DBs, etc.) client construction fails with
            # UNKNOWN_DATABASE before any user query runs. The native driver
            # never required the database to exist at connect time. Create the
            # client without a database context, then set it for subsequent
            # queries/inserts.
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.http_port,
                username=self.user,
                password=self.password,
                interface="https" if self.secure else "http",
                secure=self.secure,
                verify=bool(self.verify),
                ca_cert=self.ca_certs,
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
                settings=dict(self.client_settings),
                pool_mgr=_shared_pool(self.ca_certs, bool(self.verify)),
                # Per-call override avoids mutating shared/cached pool state.
                query_limit=self.query_limit if query_limit is None else query_limit,
                # Disable connect's built-in HTTP retries. Pool methods own the
                # retry policy (execute / execute_robust / execute_with_totals)
                # so a blip stays ~3 attempts, not 3 * (query_retries+1).
                query_retries=0,
                autogenerate_session_id=False,
                compress="lz4",
            )
            if use_database and self.database:
                client.database = self.database
            return client

    @staticmethod
    def _query_uses_database(query: str) -> bool:
        normalized = query.lstrip().upper()
        return not normalized.startswith(("CREATE DATABASE", "DROP DATABASE"))

    # Match clickhouse-connect's command classification. These statements do not
    # return a result matrix; over HTTP the driver surfaces QuerySummary fields
    # as fake columns, which snuba-admin treats as real column_names.
    _COMMAND_RE = re.compile(
        r"^\s*(CREATE|ALTER|SYSTEM|GRANT|REVOKE|CHECK|DETACH|ATTACH|DROP|"
        r"DELETE|KILL|OPTIMIZE|SET|RENAME|TRUNCATE|USE)\b",
        re.IGNORECASE,
    )

    @classmethod
    def _is_command(cls, query: str) -> bool:
        return cls._COMMAND_RE.search(query) is not None

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
            query_settings["send_logs_level"] = "trace"
        return query_settings or None

    def _consume_query_result(
        self,
        query_result: Any,
        with_column_types: bool,
        query_id: str | None,
    ) -> ClickhouseResult:
        summary = query_result.summary or {}
        read_bytes = _as_int(summary.get("read_bytes"))
        try:
            elapsed_ns = summary.get("elapsed_ns")
            elapsed = float(elapsed_ns) / 1e9 if elapsed_ns is not None else 0.0
        except (TypeError, ValueError):
            elapsed = 0.0
        profile = ClickhouseProfile(
            blocks=0,
            bytes=read_bytes,
            elapsed=elapsed,
            progress_bytes=read_bytes,
            rows=_as_int(summary.get("read_rows")),
        )
        results: Sequence[Any] = query_result.result_set
        query_id_out = str(query_result.query_id or query_id or "")
        if not with_column_types:
            return ClickhouseResult(
                results=results,
                profile=profile,
                trace_output="",
                query_id=query_id_out,
            )
        meta = [
            (name, str(column_type.name))
            for name, column_type in zip(
                query_result.column_names, query_result.column_types, strict=True
            )
        ]
        return ClickhouseResult(
            results=results,
            meta=meta,
            profile=profile,
            trace_output="",
            query_id=query_id_out,
        )

    def _execute_once(
        self,
        query: str,
        params: Params,
        with_column_types: bool,
        query_id: str | None,
        settings: Mapping[str, Any] | None,
        columnar: bool,
        capture_trace: bool,
        query_limit: int | None = None,
    ) -> ClickhouseResult:
        client = self._new_client(
            use_database=self._query_uses_database(query),
            query_limit=query_limit,
        )
        query_settings = self._build_query_settings(settings, query_id, capture_trace)

        # DDL / SYSTEM / etc. do not return a result matrix. Route them through
        # command() so QuerySummary fields are not mistaken for columns.
        if self._is_command(query):
            with _query_span(query, query_id) as span:
                span.set_attribute("settings", json.dumps(query_settings, default=repr))
                client.command(
                    query,
                    parameters=_driver_params(params),
                    settings=query_settings,
                )
            return ClickhouseResult(
                results=[],
                meta=[] if with_column_types else None,
                profile=ClickhouseProfile(blocks=0, bytes=0, elapsed=0.0, progress_bytes=0, rows=0),
                trace_output="",
                query_id=str(query_id or ""),
            )

        # Prefer JSONCompact whenever column types are required. Native HTTP
        # responses omit the column header on zero-row results, which forced a
        # second scan for meta. JSONCompact always returns meta in one request
        # (same path as WITH TOTALS). Keep Native for untyped / columnar reads.
        if with_column_types and not columnar:
            return self._execute_jsoncompact(
                client, query, params, query_id, settings, capture_trace
            )

        query_result = None
        try:
            with _query_span(query, query_id) as span:
                span.set_attribute("settings", json.dumps(query_settings, default=repr))
                query_result = client.query(
                    query,
                    parameters=_driver_params(params),
                    settings=query_settings,
                    column_oriented=columnar,
                    transport_settings=dict(_CLICKHOUSE_CONNECT_TRANSPORT_SETTINGS),
                )
            return self._consume_query_result(query_result, with_column_types, query_id)
        finally:
            if query_result is not None:
                with suppress(Exception):
                    query_result.close()

    def _execute_jsoncompact(
        self,
        client: Client,
        query: str,
        params: Params,
        query_id: str | None,
        settings: Mapping[str, Any] | None,
        capture_trace: bool,
    ) -> ClickhouseResult:
        json_settings: dict[str, Any] = dict(settings) if settings else {}
        json_settings["output_format_json_quote_64bit_integers"] = 0
        if query_id is not None:
            json_settings["query_id"] = query_id
        if capture_trace:
            json_settings["send_logs_level"] = "trace"
        query = _apply_client_query_limit(client, query)

        with _query_span(query, query_id):
            raw = client.raw_query(
                query,
                parameters=_driver_params(params),
                settings=json_settings,
                fmt="JSONCompact",
            )
        payload = json.loads(raw)
        meta = [(column["name"], column["type"]) for column in payload.get("meta", [])]
        column_types = [ch_type for _, ch_type in meta]

        def _row(values: Sequence[Any]) -> tuple[Any, ...]:
            return tuple(_coerce_temporal(value, column_types[i]) for i, value in enumerate(values))

        results = [_row(row) for row in payload.get("data", [])]
        return ClickhouseResult(
            results=results,
            meta=meta,
            profile=self._profile_from_statistics(payload),
            trace_output="",
            query_id=str(payload.get("query_id") or query_id or ""),
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
        # raw_query sits outside execute(); still needs the same transport
        # retries now that connect's query_retries are disabled.
        def _once() -> ClickhouseResult:
            client = self._new_client()
            json_settings: dict[str, Any] = dict(settings) if settings else {}
            json_settings["output_format_json_quote_64bit_integers"] = 0
            if query_id is not None:
                json_settings["query_id"] = query_id
            if capture_trace:
                json_settings["send_logs_level"] = "trace"

            with _query_span(query, query_id):
                raw = client.raw_query(
                    query,
                    parameters=_driver_params(params),
                    settings=json_settings,
                    fmt="JSONCompact",
                )

            payload = json.loads(raw)
            meta = [(column["name"], column["type"]) for column in payload.get("meta", [])]
            column_types = [ch_type for _, ch_type in meta]

            def _row(values: Sequence[Any]) -> tuple[Any, ...]:
                return tuple(
                    _coerce_temporal(value, column_types[i]) for i, value in enumerate(values)
                )

            results = [_row(row) for row in payload.get("data", [])]
            totals = payload.get("totals")
            if totals:
                results.append(_row(totals))
            return ClickhouseResult(
                results=results,
                meta=meta,
                profile=self._profile_from_statistics(payload),
                trace_output="",
            )

        if robust:
            return self._retry_robust(_once)
        return self._retry_transport(_once, retryable=True)

    @staticmethod
    def _profile_from_statistics(payload: Mapping[str, Any]) -> ClickhouseProfile:
        statistics = payload.get("statistics") or {}
        read_bytes = _as_int(statistics.get("bytes_read") or 0)
        return ClickhouseProfile(
            blocks=0,
            bytes=read_bytes,
            elapsed=_as_float(statistics.get("elapsed") or 0.0),
            progress_bytes=read_bytes,
            rows=_as_int(statistics.get("rows_read") or 0),
        )

    @contextmanager
    def _translate_clickhouse_errors(self) -> Iterator[None]:
        try:
            yield
        except OperationalError as e:
            metrics.increment("connection_error")
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e
        except StreamFailureError as e:
            metrics.increment("stream_failure")
            raise ClickhouseError(str(e), code=-1) from e
        except ClickHouseError as e:
            # Missing server code is not a transport failure. -1 is reserved
            # for OperationalError / StreamFailureError / malformed HTTP bodies.
            code = getattr(e, "code", None)
            raise ClickhouseError(str(e), code=code if isinstance(code, int) else 0) from e
        except json.JSONDecodeError as e:
            raise ClickhouseError(f"invalid JSON response: {e}", code=-1) from e

    def _retry_transport(
        self,
        operation: Callable[[], ClickhouseResult],
        *,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """Retry transient connect/socket errors up to three times."""
        attempts_remaining = 3 if retryable else 1
        while True:
            try:
                with self._translate_clickhouse_errors():
                    return operation()
            except ClickhouseError as e:
                if e.code not in _RETRYABLE_TRANSPORT_CODES:
                    raise
                attempts_remaining -= 1
                if attempts_remaining <= 0:
                    raise
                # Short sleep so a load balancer can mark a bad host down,
                # matching the old native pool.
                time.sleep(0.1)

    def _retry_robust(
        self,
        operation: Callable[[], ClickhouseResult],
        *,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """Retry transport failures and TOO_MANY_SIMULTANEOUS_QUERIES."""
        total_attempts = 3 if retryable else 1
        attempts_remaining = total_attempts

        while True:
            try:
                # Nested transport retries are off: this loop owns both policies.
                return self._retry_transport(operation, retryable=False)
            except ClickhouseError as e:
                logger.warning(
                    "ClickHouse query execution failed: %s (%d tries left)",
                    str(e),
                    attempts_remaining,
                )
                if e.code == ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES:
                    attempts_remaining -= 1
                    if attempts_remaining <= 0:
                        raise
                    # Linear backoff. Falls back to a 1-second base when the
                    # option is unset (0), matching the old native pool.
                    sleep_interval_seconds = (
                        get_option("simultaneous_queries_sleep_seconds", 0) or 1
                    )
                    time.sleep(
                        float((total_attempts - attempts_remaining) * sleep_interval_seconds)
                    )
                    continue
                if e.code in _RETRYABLE_TRANSPORT_CODES:
                    attempts_remaining -= 1
                    if attempts_remaining <= 0:
                        raise
                    time.sleep(1)
                    continue
                raise

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
        query_limit: int | None = None,
    ) -> ClickhouseResult:
        """
        Execute a ClickHouse query with a quick transport-failure retry.

        Matches the old native pool: when ``retryable`` is true, transient
        connect/socket errors are retried up to three times with a short sleep
        so callers like cluster discovery can ride out a blip. Server errors
        (including ``TOO_MANY_SIMULTANEOUS_QUERIES``) are not retried here;
        use :meth:`execute_robust` for that.
        """
        return self._retry_transport(
            lambda: self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
                query_limit=query_limit,
            ),
            retryable=retryable,
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
        insert_settings: dict[str, Any] = dict(settings) if settings else {}
        if query_id is not None:
            insert_settings["query_id"] = query_id

        with self._translate_clickhouse_errors():
            client = self._new_client()
            with _query_span(
                _insert_statement(table, column_names),
                query_id if query_id is not None else "unknown-query-id",
                name=f"INSERT INTO {table}",
            ):
                client.insert(
                    table,
                    matrix,
                    column_names=column_names,
                    database=self.database,
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
        Execute a ClickHouse query with more tenacity than :meth:`execute`.

        Retries transport failures and ``TOO_MANY_SIMULTANEOUS_QUERIES`` so
        critical paths (e.g. the replacer) can finish under transient load
        instead of failing immediately. Other server errors are raised as-is.
        """
        return self._retry_robust(
            lambda: self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
            ),
            retryable=retryable,
        )

    def execute_explain(self, query: str) -> ClickhouseResult:
        with self._translate_clickhouse_errors():
            client = self._new_client()
            with _query_span(query):
                output = client.command(query)
            return self._explain_result(output)

    @staticmethod
    def _explain_result(output: object) -> ClickhouseResult:
        if isinstance(output, str):
            text = output
        elif isinstance(output, int):
            text = str(output)
        elif isinstance(output, (list, tuple)):
            text = "\t".join(str(part) for part in output)
        else:
            text = ""
        results: list[tuple[str, ...]] = [(line,) for line in text.split("\n")] if text else []
        return ClickhouseResult(
            results=results,
            meta=[("explain", "String")],
            profile=ClickhouseProfile(
                bytes=0, progress_bytes=0, blocks=0, rows=len(results), elapsed=0.0
            ),
            trace_output="",
        )

    def close(self) -> None:
        pass
