from __future__ import annotations

import logging
import queue
import random
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import partial
from io import StringIO
from typing import (
    Any,
    Dict,
    Generator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
    cast,
)
from uuid import UUID

import sentry_sdk
from clickhouse_driver import Client, errors
from dateutil.tz import tz

from snuba import environment, settings, state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.reader import Reader, Result, build_result_transformer
from snuba.utils.metrics.gauge import ThreadSafeGauge
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.clickhouse")
trace_logger = logging.getLogger("clickhouse_driver.log")
trace_logger.setLevel("INFO")

Params = Optional[Union[Sequence[Any], Mapping[str, Any]]]

metrics = MetricsWrapper(environment.metrics, "clickhouse.native")


class ClickhouseProfile(TypedDict):
    bytes: int
    blocks: int
    rows: int
    elapsed: float


@dataclass(frozen=True)
class ClickhouseResult:
    results: Sequence[Any] = field(default_factory=list)
    meta: Sequence[Any] | None = None
    profile: ClickhouseProfile | None = None
    trace_output: str = ""


@contextmanager
def capture_logging() -> Generator[StringIO, None, None]:
    buffer = StringIO()
    new_handler = logging.StreamHandler(buffer)
    trace_logger.addHandler(new_handler)

    yield buffer

    trace_logger.removeHandler(new_handler)
    buffer.close()


class ClickhousePool(object):
    FALLBACK_POOL_SIZE = 3

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connect_timeout: int = 1,
        send_receive_timeout: Optional[int] = 300,
        max_pool_size: int = settings.CLICKHOUSE_MAX_POOL_SIZE,
        client_settings: Mapping[str, Any] = {},
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.client_settings = client_settings

        self.pool: queue.LifoQueue[Optional[Client]] = queue.LifoQueue(max_pool_size)
        self.fallback_pool: queue.LifoQueue[Optional[Client]] = queue.LifoQueue(
            self.FALLBACK_POOL_SIZE
        )
        self.__gauge = ThreadSafeGauge(metrics, "connections")

        # Fill the queue up so that doing get() on it will block properly
        for _ in range(max_pool_size):
            self.pool.put(None)

        for _ in range(self.FALLBACK_POOL_SIZE):
            self.fallback_pool.put(None)

    def fallback_pool_enabled(self) -> bool:
        return state.get_config("use_fallback_host_in_native_connection_pool", 0) == 1

    def get_fallback_host(self) -> Tuple[str, int]:
        config_hosts_str = state.get_config(
            f"fallback_hosts:{self.host}:{self.port}", None
        )
        assert config_hosts_str, f"no fallback hosts found for {self.host}:{self.port}"

        config_hosts = cast(str, config_hosts_str).split(",")
        selected_host_port = random.choice(config_hosts).split(":")

        assert (
            len(selected_host_port) == 2
        ), f"expected host:port format in fallback hosts for {self.host}:{self.port}"

        return (selected_host_port[0], int(selected_host_port[1]))

    # This will actually return an int if an INSERT query is run, but we never capture the
    # output of INSERT queries so I left this as a Sequence.
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
        Execute a clickhouse query with a single quick retry in case of
        connection failure.

        This should smooth over any Clickhouse instance restarts, but will also
        return relatively quickly with an error in case of more persistent
        failures.
        """
        fallback_mode = False

        try:
            conn = self.pool.get(block=True)

            if retryable:
                attempts_remaining = 3 + (1 if self.fallback_pool_enabled() else 0)
            else:
                attempts_remaining = 1

            while attempts_remaining > 0:
                attempts_remaining -= 1
                # Lazily create connection instances
                if conn is None:
                    self.__gauge.increment()
                    conn = self._create_conn(fallback_mode)

                try:
                    if capture_trace:
                        settings = (
                            {**settings, "send_logs_level": "trace"}
                            if settings
                            else {"send_logs_level": "trace"}
                        )

                    query_execute = partial(
                        conn.execute,
                        query,
                        params=params,
                        with_column_types=with_column_types,
                        query_id=query_id,
                        settings=settings,
                        types_check=types_check,
                        columnar=columnar,
                    )
                    result_data: Sequence[Any]
                    trace_output = ""
                    with sentry_sdk.start_span(description=query, op="db.clickhouse") as span:
                        span.set_data(sentry_sdk.consts.SPANDATA.DB_SYSTEM, "clickhouse")
                        if capture_trace:
                            with capture_logging() as buffer:
                                result_data = query_execute()
                                trace_output = buffer.getvalue()
                        else:
                            result_data = query_execute()

                    profile_data = ClickhouseProfile(
                        bytes=conn.last_query.profile_info.bytes or 0,
                        blocks=conn.last_query.profile_info.blocks or 0,
                        rows=conn.last_query.profile_info.rows or 0,
                        elapsed=conn.last_query.elapsed or 0.0,
                    )
                    if with_column_types:
                        result = ClickhouseResult(
                            results=result_data[0],
                            meta=result_data[1],
                            profile=profile_data,
                            trace_output=trace_output,
                        )
                    else:
                        if not isinstance(result_data, (list, tuple)):
                            result_data = [result_data]
                        result = ClickhouseResult(
                            results=result_data,
                            profile=profile_data,
                            trace_output=trace_output,
                        )

                    return result
                except (errors.NetworkError, errors.SocketTimeoutError, EOFError) as e:
                    metrics.increment(
                        "connection_error"
                        if not fallback_mode
                        else "fallback_connection_error"
                    )

                    # Force a reconnection next time
                    conn = None
                    self.__gauge.decrement()

                    # Move to fallback-mode for one last try if it's enabled
                    if attempts_remaining == 1 and self.fallback_pool_enabled():
                        # return a client instance placeholder back to the main connection pool
                        self.pool.put(None, block=False)
                        # turn fallback mode on (so new connections will come from run-time config)
                        fallback_mode = True
                        # try reusing a connection from the fallback connection pool, but if
                        # it's None we'll create the connection on-demand later
                        conn = self.fallback_pool.get(block=True)
                    else:
                        if attempts_remaining == 0:
                            if isinstance(e, errors.Error):
                                raise ClickhouseError(e.message, code=e.code) from e
                            else:
                                raise e
                        else:
                            # Short sleep to make sure we give the load
                            # balancer a chance to mark a bad host as down.
                            time.sleep(0.1)
                except errors.Error as e:
                    if e.code == errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES:
                        attempts_remaining -= 1
                        if attempts_remaining <= 0:
                            raise ClickhouseError(e.message, code=e.code) from e

                        sleep_interval_seconds = state.get_config(
                            "simultaneous_queries_sleep_seconds", None
                        )
                        if not sleep_interval_seconds:
                            raise ClickhouseError(e.message, code=e.code) from e

                        attempts_remaining = min(
                            attempts_remaining, 1
                        )  # only retry once

                        assert sleep_interval_seconds is not None
                        # Linear backoff. Adds one second at each iteration.
                        time.sleep(sleep_interval_seconds)
                        continue

                    raise ClickhouseError(e.message, code=e.code) from e
        finally:
            # Return finished connection to the appropriate connection pool
            if not fallback_mode:
                self.pool.put(conn, block=False)
            else:
                self.fallback_pool.put(conn, block=False)

        return ClickhouseResult()

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
        Execute a clickhouse query with a bit more tenacity. Make more retry
        attempts, (infinite in the case of too many simultaneous queries
        errors) and wait a second between retries.

        This is by components which need to either complete their current
        query successfully or else quit altogether. Note that each retry in this
        loop will be doubled by the retry in execute()
        """
        total_attempts = 3 if retryable else 1
        attempts_remaining = total_attempts

        while True:
            try:
                return self.execute(
                    query,
                    params=params,
                    with_column_types=with_column_types,
                    query_id=query_id,
                    settings=settings,
                    types_check=types_check,
                    columnar=columnar,
                    capture_trace=capture_trace,
                )
            except (errors.NetworkError, errors.SocketTimeoutError, EOFError) as e:
                # Try 3 times on connection issues.
                logger.warning(
                    "ClickHouse query execution failed: %s (%d tries left)",
                    str(e),
                    attempts_remaining,
                )
                attempts_remaining -= 1
                if attempts_remaining <= 0:
                    if isinstance(e, errors.Error):
                        raise ClickhouseError(e.message, code=e.code) from e
                    else:
                        raise e
                time.sleep(1)
                continue
            except ClickhouseError as e:
                logger.warning(
                    "ClickHouse query execution failed: %s (%d tries left)",
                    str(e),
                    attempts_remaining,
                )
                if e.code == errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES:
                    attempts_remaining -= 1
                    if attempts_remaining <= 0:
                        raise e
                    sleep_interval_seconds = state.get_config(
                        "simultaneous_queries_sleep_seconds", 1
                    )
                    assert sleep_interval_seconds is not None
                    # Linear backoff. Adds one second at each iteration.
                    time.sleep(
                        float(
                            (total_attempts - attempts_remaining)
                            * sleep_interval_seconds
                        )
                    )
                    continue
                else:
                    # Quit immediately for other types of server errors.
                    raise e
            except errors.Error as e:
                raise ClickhouseError(e.message, code=e.code) from e

    def _create_conn(self, use_fallback_host: bool = False) -> Client:
        if use_fallback_host:
            (fallback_host, fallback_port) = self.get_fallback_host()
        return Client(
            host=(self.host if not use_fallback_host else fallback_host),
            port=(self.port if not use_fallback_host else fallback_port),
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            settings=self.client_settings,
        )

    def close(self) -> None:
        try:
            while True:
                conn = self.pool.get(block=False)
                if conn:
                    conn.disconnect()
        except queue.Empty:
            pass


def transform_date(value: date) -> str:
    """
    Convert a timezone-naive date object into an ISO 8601 formatted date and
    time string respresentation.
    """
    # XXX: Both Python and ClickHouse date objects do not have time zones, so
    # just assume UTC. (Ideally, we'd have just left these as timezone naive to
    # begin with and not done this transformation at all, since the time
    # portion has no benefit or significance here.)
    return datetime(*value.timetuple()[:6]).replace(tzinfo=tz.tzutc()).isoformat()


def transform_datetime(value: datetime) -> str:
    """
    Convert a timezone-naive datetime object into an ISO 8601 formatted date
    and time string representation.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz.tzutc())
    else:
        value = value.astimezone(tz.tzutc())
    return value.isoformat()


def transform_uuid(value: UUID) -> str:
    """
    Convert a UUID object into a string representation.
    """
    return str(value)


transform_column_types = build_result_transformer(
    [
        (re.compile(r"^Date(\(.+\))?$"), transform_date),
        (re.compile(r"^DateTime(\(.+\))?$"), transform_datetime),
        (re.compile(r"^UUID$"), transform_uuid),
    ]
)


class NativeDriverReader(Reader):
    def __init__(
        self,
        cache_partition_id: Optional[str],
        client: ClickhousePool,
        query_settings_prefix: Optional[str],
    ) -> None:
        super().__init__(
            cache_partition_id=cache_partition_id,
            query_settings_prefix=query_settings_prefix,
        )
        self.__client = client

    def __transform_result(self, result: ClickhouseResult, with_totals: bool) -> Result:
        """
        Transform a native driver response into a response that is
        structurally similar to a ClickHouse-flavored JSON response.
        """
        meta = result.meta if result.meta is not None else []
        data = result.results
        profile = cast(Optional[Dict[str, Any]], result.profile)
        # XXX: Rows are represented as mappings that are keyed by column or
        # alias, which is problematic when the result set contains duplicate
        # names. To ensure that the column headers and row data are consistent
        # duplicated names are discarded at this stage.
        columns = {c[0]: i for i, c in enumerate(meta)}

        data = [
            {column: row[index] for column, index in columns.items()} for row in data
        ]

        meta = [
            {"name": m[0], "type": m[1]} for m in [meta[i] for i in columns.values()]
        ]

        new_result: Result = {}
        if with_totals:
            assert len(data) > 0
            totals = data.pop(-1)
            new_result = {
                "data": data,
                "meta": meta,
                "totals": totals,
                "profile": profile,
                "trace_output": result.trace_output,
            }
        else:
            new_result = {
                "data": data,
                "meta": meta,
                "profile": profile,
                "trace_output": result.trace_output,
            }

        transform_column_types(new_result)

        return new_result

    def execute(
        self,
        query: FormattedQuery,
        # TODO: move Clickhouse specific arguments into clickhouse.query.Query
        settings: Optional[Mapping[str, str]] = None,
        with_totals: bool = False,
        robust: bool = False,
        capture_trace: bool = False,
    ) -> Result:
        settings = {**settings} if settings is not None else {}

        query_id = None
        if "query_id" in settings:
            query_id = settings.pop("query_id")

        execute_func = (
            self.__client.execute_robust if robust is True else self.__client.execute
        )

        return self.__transform_result(
            execute_func(
                query.get_sql(),
                with_column_types=True,
                query_id=query_id,
                settings=settings,
                capture_trace=capture_trace,
            ),
            with_totals=with_totals,
        )
