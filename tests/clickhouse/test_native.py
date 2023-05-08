import queue
from datetime import datetime, timedelta
from typing import Any, Callable
from unittest import mock

import pytest
from clickhouse_driver import errors
from dateutil.tz import tz

from snuba import state
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import ClickhousePool, transform_datetime


def test_transform_datetime() -> None:
    now = datetime(2020, 1, 2, 3, 4, 5)
    fmt = "2020-01-02T03:04:05+00:00"
    assert transform_datetime(now) == fmt
    assert transform_datetime(now.replace(tzinfo=tz.tzutc())) == fmt

    offset = timedelta(hours=8)
    assert (
        transform_datetime(now.replace(tzinfo=tz.tzoffset("PST", offset)) + offset)
        == fmt
    )


@pytest.mark.redis_db
def test_robust_concurrency_limit() -> None:
    connection = mock.Mock()
    connection.execute.side_effect = ClickhouseError(
        "some error", extra_data={"code": 1}
    )

    pool = ClickhousePool("host", 100, "test", "test", "test")
    pool.pool = queue.LifoQueue(1)
    pool.pool.put(connection, block=False)

    with pytest.raises(ClickhouseError):
        pool.execute_robust("SELECT something")
    connection.execute.assert_called_once()

    connection.reset_mock(side_effect=True)
    connection.execute.side_effect = ClickhouseError(
        "some error",
        code=errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES,
    )

    with pytest.raises(ClickhouseError):
        pool.execute_robust("SELECT something")
    assert connection.execute.call_count == 3, "Expected three attempts"


class TestError(errors.Error):  # type: ignore
    code = 1


class TestConcurrentError(errors.Error):  # type: ignore
    code = errors.ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES


@pytest.mark.redis_db
def test_concurrency_limit() -> None:
    connection = mock.Mock()
    connection.execute.side_effect = TestError("some error")

    state.set_config("simultaneous_queries_sleep_seconds", 0.5)

    pool = ClickhousePool("host", 100, "test", "test", "test")
    pool.pool = queue.LifoQueue(1)
    pool.pool.put(connection, block=False)

    with pytest.raises(ClickhouseError):
        pool.execute("SELECT something")
    connection.execute.assert_called_once()

    connection.reset_mock(side_effect=True)
    connection.execute.side_effect = TestConcurrentError("some error")

    with pytest.raises(ClickhouseError):
        pool.execute("SELECT something")
    assert connection.execute.call_count == 2, "Expected two attempts"


TEST_DB_NAME = "test"
CLUSTER_HOST = "host"
CLUSTER_PORT = 100


@pytest.mark.redis_db
def test_get_fallback_host() -> None:
    FALLBACK_HOSTS_CONFIG_VAL = "host1:100,host2:100,host3:100"
    FALLBACK_HOSTS = [("host1", 100), ("host2", 100), ("host3", 100)]

    state.set_config(
        f"fallback_hosts:{CLUSTER_HOST}:{CLUSTER_PORT}", FALLBACK_HOSTS_CONFIG_VAL
    )

    pool = ClickhousePool(CLUSTER_HOST, CLUSTER_PORT, "test", "test", TEST_DB_NAME)

    assert pool.get_fallback_host() in FALLBACK_HOSTS


@pytest.mark.redis_db
def test_fallback_logic() -> None:
    state.set_config("use_fallback_host_in_native_connection_pool", 1)

    network_failure_connection = mock.Mock()
    network_failure_connection.execute.side_effect = EOFError()

    verification_connection = mock.Mock()
    verification_connection.execute.return_value = []

    pool = ClickhousePool(CLUSTER_HOST, CLUSTER_PORT, "test", "test", TEST_DB_NAME)

    # The execute method will try to reuse a single slot in the connection
    # pool but reestablish new connections with _create_conn if a connection
    # fails with a network-related error. It may be cleaner to move connection
    # negotation/establishment into another class for separation of concerns.
    with mock.patch.object(
        pool, "_create_conn", lambda x, y=False: network_failure_connection
    ):
        pool.pool = queue.LifoQueue(1)
        pool.pool.put(network_failure_connection, block=False)
        pool.fallback_pool = queue.LifoQueue(1)
        pool.fallback_pool.put(verification_connection, block=False)
        pool.execute("SELECT something")

    assert (
        network_failure_connection.execute.call_count == 3
    ), "Expected three (failed) attempts with main connection pool"
    assert (
        verification_connection.execute.call_count == 1
    ), "Expected one (successful) attempt with fallback connection pool"


def teardown_function(_: Callable[..., Any]) -> None:
    state.delete_config("use_fallback_host_in_native_connection_pool")
    state.delete_config(f"fallback_hosts:{CLUSTER_HOST}:{CLUSTER_PORT}")


@pytest.mark.parametrize(
    "retryable, expected",
    [
        pytest.param(True, 3, id="retries"),
        pytest.param(False, 1, id="no retries"),
    ],
)
@pytest.mark.redis_db
def test_execute_retries(retryable: bool, expected: int) -> None:
    socket_timeout_connection = mock.Mock()
    socket_timeout_connection.execute.side_effect = errors.SocketTimeoutError

    pool = ClickhousePool(CLUSTER_HOST, CLUSTER_PORT, "test", "test", TEST_DB_NAME)

    with mock.patch.object(
        pool, "_create_conn", lambda x, y=False: socket_timeout_connection
    ):
        pool.pool = queue.LifoQueue(1)
        pool.pool.put(socket_timeout_connection, block=False)
        with pytest.raises(ClickhouseError):
            pool.execute("SELECT something", retryable=retryable)

    assert (
        socket_timeout_connection.execute.call_count == expected
    ), f"Expected {expected} (failed) attempts with main connection pool"
