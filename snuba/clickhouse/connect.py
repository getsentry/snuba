from __future__ import annotations

import logging
from threading import Lock
from typing import Any, Mapping, Optional, Sequence

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
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.clickhouse.connect")

metrics = MetricsWrapper(environment.metrics, "clickhouse.connect")

# The native pool caps its send/receive timeout at 35s; for the HTTP path we
# cap it at 30s. Any larger (or unset) timeout coming from a client settings
# profile is clamped down to this value.
MAX_SEND_RECEIVE_TIMEOUT_SECONDS = 30

# clickhouse-connect raises a ProgrammingError by default when it is asked to
# send a setting it considers unknown or readonly. The native driver simply
# forwards whatever settings it is given to the server, so to preserve parity
# we tell clickhouse-connect to drop unrecognized settings instead of failing.
clickhouse_connect_common.set_setting("invalid_setting_action", "drop")


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
        http_port: int,
        user: str,
        password: str,
        database: str,
        secure: bool = False,
        ca_certs: Optional[str] = None,
        verify: Optional[bool] = False,
        connect_timeout: int = 1,
        send_receive_timeout: Optional[int] = 35,
        max_pool_size: int = settings.CLICKHOUSE_MAX_POOL_SIZE,
        client_settings: Mapping[str, Any] = {},
    ) -> None:
        # Intentionally does not call ClickhousePool.__init__: the native queue
        # of connections is not used here. ``port`` mirrors the base class
        # attribute (it holds the HTTP port for this driver).
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
        self.max_pool_size = max_pool_size
        self.client_settings = client_settings

        self.__client: Optional[Client] = None
        self.__lock = Lock()

    def _get_client(self) -> Client:
        # The client (and its handshake with the server) is created lazily so
        # that simply constructing a pool does not open a connection.
        if self.__client is None:
            with self.__lock:
                if self.__client is None:
                    # Default to the configured CLICKHOUSE_MAX_POOL_SIZE, but
                    # allow it to be overridden at runtime. The value is read
                    # once when the (cached) client is first created.
                    pool_size = (
                        state.get_int_config("clickhouse_connect_pool_size", self.max_pool_size)
                        or self.max_pool_size
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
                        connect_timeout=min(self.connect_timeout, MAX_SEND_RECEIVE_TIMEOUT_SECONDS),
                        # Cap the read timeout at 30s regardless of what the
                        # client settings profile asks for (some profiles, e.g.
                        # MIGRATE, use very large timeouts that are not
                        # appropriate for the HTTP query path).
                        send_receive_timeout=min(
                            self.send_receive_timeout
                            if self.send_receive_timeout is not None
                            else MAX_SEND_RECEIVE_TIMEOUT_SECONDS,
                            MAX_SEND_RECEIVE_TIMEOUT_SECONDS,
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
        settings: Optional[Mapping[str, Any]],
        query_id: Optional[str],
        capture_trace: bool,
    ) -> Optional[Mapping[str, Any]]:
        query_settings = dict(settings) if settings else {}
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
        query_id: Optional[str],
        settings: Optional[Mapping[str, Any]],
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

        if with_column_types:
            meta = [
                (name, column_type.name)
                for name, column_type in zip(query_result.column_names, query_result.column_types)
            ]
            return ClickhouseResult(
                results=results,
                meta=meta,
                profile=profile_data,
                trace_output="",
            )

        return ClickhouseResult(
            results=results,
            profile=profile_data,
            trace_output="",
        )

    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: Optional[str] = None,
        settings: Optional[Mapping[str, Any]] = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        """
        Execute a clickhouse query.

        Unlike :class:`snuba.clickhouse.native.ClickhousePool`, this method
        does not implement any retry logic of its own. Retries (stale
        keep-alive sockets, transport errors and HTTP 429/503/504 responses)
        are handled internally by clickhouse-connect. Notably this means the
        native pool's ``TOO_MANY_SIMULTANEOUS_QUERIES`` backoff is *not*
        replicated: clickhouse-connect does not retry that error, so it is
        surfaced directly to the caller.

        The ``retryable`` argument is accepted for interface parity with the
        native pool but has no effect here.
        """
        try:
            return self._execute_once(
                query,
                params,
                with_column_types,
                query_id,
                settings,
                columnar,
                capture_trace,
            )
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
            # error (DatabaseError, ProgrammingError, DataError, ...). The
            # native pool likewise wraps the whole clickhouse_driver errors.Error
            # family into ClickhouseError, preserving the server error code when
            # there is one.
            raise ClickhouseError(str(e), code=getattr(e, "code", None) or -1) from e

    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: Optional[str] = None,
        settings: Optional[Mapping[str, Any]] = None,
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

    def close(self) -> None:
        if self.__client is not None:
            self.__client.close()
            self.__client = None
