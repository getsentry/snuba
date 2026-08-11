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


def _driver_params(params: Params) -> Sequence[Any] | dict[str, Any] | None:
    if not params:
        return None
    if isinstance(params, Mapping):
        return dict(params)
    return params


def _insert_statement(table: str, column_names: Sequence[str]) -> str:
    columns = ", ".join(quote_identifier(name) for name in column_names)
    return f"INSERT INTO {table} ({columns}) FORMAT Native"


# clickhouse-connect cannot take None for read timeout (progress-interval math).
UNBOUNDED_SEND_RECEIVE_TIMEOUT_SECONDS = 86_400  # 24h
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123

clickhouse_connect_common.set_setting("invalid_setting_action", "drop")
clickhouse_connect_common.set_setting(
    "use_protocol_version",
    get_option("clickhouse_connect_use_protocol_version", False),
)

# One socket pool for the process.
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
                num_pools=64,
            )
            _pool_managers[key] = manager
        return manager


def _coerce_temporal(value: Any, ch_type: str) -> Any:
    if not isinstance(value, str):
        return value
    _, inner = unwrap_nullable_type(ch_type)
    if not inner.startswith("Date"):
        return value
    parsed = datetime.fromisoformat(value)
    return parsed if inner.startswith("DateTime") else parsed.date()


class ClickhouseConnectPool(ClickhousePool):
    """HTTP ClickHouse driver (clickhouse-connect)."""

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

    def _new_client(self) -> Client:
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
            pool_mgr=_shared_pool(self.ca_certs, bool(self.verify)),
            query_limit=0,
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
        client = self._new_client()
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
            meta = [
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
        finally:
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
            results: list[tuple[Any, ...]] = [
                tuple(_coerce_temporal(value, column_types[i]) for i, value in enumerate(row))
                for row in payload.get("data", [])
            ]
            totals = payload.get("totals")
            if totals:
                results.append(
                    tuple(
                        _coerce_temporal(value, column_types[i]) for i, value in enumerate(totals)
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
        if isinstance(output, str):
            text = output
        elif isinstance(output, int):
            text = str(output)
        elif isinstance(output, (list, tuple)):
            text = "\t".join(str(part) for part in output)
        else:
            text = ""
        results: list[tuple[str, ...]] = [(line,) for line in text.split("\n")] if text else []
        profile = ClickhouseProfile(
            bytes=0, progress_bytes=0, blocks=0, rows=len(results), elapsed=0.0
        )
        return ClickhouseResult(
            results=results, meta=[("explain", "String")], profile=profile, trace_output=""
        )

    def close(self) -> None:
        pass
