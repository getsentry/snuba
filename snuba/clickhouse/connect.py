from __future__ import annotations

import atexit
import json
import os
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
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.sentry import SENTRY_OP

metrics = MetricsWrapper(environment.metrics, "clickhouse.connect")

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
        self.query_limit = query_limit

    def _new_client(self, query_limit: int | None = None) -> Client:
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
                "server.port": str(self.port),
            },
        ):
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
                pool_mgr=_shared_pool(self.ca_certs, bool(self.verify)),
                # Per-call override avoids mutating shared/cached pool state.
                query_limit=self.query_limit if query_limit is None else query_limit,
                autogenerate_session_id=False,
                compress="lz4",
            )

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
        self, query_result: Any, with_column_types: bool, query_id: str | None
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
            (name, column_type.name)
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
        client = self._new_client(query_limit=query_limit)
        query_settings = self._build_query_settings(settings, query_id, capture_trace)

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

    def execute_with_totals(
        self,
        query: str,
        params: Params = None,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        capture_trace: bool = False,
        robust: bool = False,
    ) -> ClickhouseResult:
        with self._translate_clickhouse_errors():
            client = self._new_client()
            json_settings: dict[str, Any] = dict(settings) if settings else {}
            json_settings["output_format_json_quote_64bit_integers"] = 0
            if query_id is not None:
                json_settings["query_id"] = query_id

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
        query_limit: int | None = None,
    ) -> ClickhouseResult:
        with self._translate_clickhouse_errors():
            return self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
                query_limit=query_limit,
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
