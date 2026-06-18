from typing import Any
from unittest import mock

import pytest

from snuba.clickhouse.connect import (
    ClickhouseConnectPool,
    DriverSelectingPool,
)
from snuba.clickhouse.errors import ClickhouseError

# Error code returned by ClickHouse when the maximum number of simultaneous
# queries has been exceeded.
TOO_MANY_SIMULTANEOUS_QUERIES = 202


class FakeColumnType:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeQueryResult:
    def __init__(
        self,
        result_set: Any,
        column_names: Any = (),
        column_types: Any = (),
        summary: Any = None,
    ) -> None:
        self.result_set = result_set
        self.column_names = column_names
        self.column_types = column_types
        self.summary = summary or {}


def _make_pool(client: mock.Mock) -> ClickhouseConnectPool:
    pool = ClickhouseConnectPool(
        host="host",
        http_port=8123,
        user="test",
        password="test",
        database="test",
    )
    # Avoid creating a real client / connection.
    pool._get_client = lambda: client  # type: ignore[method-assign]
    return pool


def test_execute_maps_result_and_profile() -> None:
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(
        result_set=[[1, "a"], [2, "b"]],
        column_names=("id", "name"),
        column_types=(FakeColumnType("UInt64"), FakeColumnType("String")),
        summary={"read_rows": "2", "read_bytes": "128", "elapsed_ns": "1500000000"},
    )

    pool = _make_pool(client)
    result = pool.execute("SELECT id, name FROM t", with_column_types=True)

    assert result.results == [[1, "a"], [2, "b"]]
    assert result.meta == [("id", "UInt64"), ("name", "String")]
    assert result.profile is not None
    assert result.profile["rows"] == 2
    assert result.profile["bytes"] == 128
    assert result.profile["elapsed"] == 1.5


def test_execute_passes_query_id_and_settings() -> None:
    client = mock.Mock()
    client.query.return_value = FakeQueryResult(result_set=[])

    pool = _make_pool(client)
    pool.execute(
        "SELECT 1",
        query_id="my-query-id",
        settings={"max_threads": 4},
    )

    _, kwargs = client.query.call_args
    assert kwargs["settings"]["query_id"] == "my-query-id"
    assert kwargs["settings"]["max_threads"] == 4


def test_too_many_simultaneous_queries_not_retried() -> None:
    # We delegate all retries to clickhouse-connect, which does not retry the
    # TOO_MANY_SIMULTANEOUS_QUERIES error. It should be surfaced directly,
    # mapped to a ClickhouseError that preserves the error code.
    from clickhouse_connect.driver.exceptions import DatabaseError

    client = mock.Mock()
    client.query.side_effect = DatabaseError("too many", code=TOO_MANY_SIMULTANEOUS_QUERIES)

    pool = _make_pool(client)
    try:
        pool.execute("SELECT 1")
        raise AssertionError("expected a ClickhouseError to be raised")
    except ClickhouseError as error:
        assert error.code == TOO_MANY_SIMULTANEOUS_QUERIES
    # No retry on top of clickhouse-connect's own handling.
    assert client.query.call_count == 1


def test_operational_error_mapped_without_extra_retries() -> None:
    # Connection-level retries are clickhouse-connect's responsibility; we only
    # map the surfaced error onto ClickhouseError without retrying again.
    from clickhouse_connect.driver.exceptions import OperationalError

    client = mock.Mock()
    client.query.side_effect = OperationalError("connection refused")

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute("SELECT 1", retryable=True)

    assert client.query.call_count == 1


def test_generic_clickhouse_error_wrapped() -> None:
    # Any clickhouse-connect error (here a ProgrammingError) must be wrapped in
    # a snuba ClickhouseError, matching how the native pool wraps the whole
    # clickhouse_driver errors.Error family.
    from clickhouse_connect.driver.exceptions import ProgrammingError

    client = mock.Mock()
    client.query.side_effect = ProgrammingError("bad query")

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute("SELECT 1")

    assert client.query.call_count == 1


def test_send_receive_timeout_capped_at_30s() -> None:
    import clickhouse_connect

    pool = ClickhouseConnectPool(
        host="host",
        http_port=8123,
        user="test",
        password="test",
        database="test",
        connect_timeout=60,
        # A large timeout (e.g. coming from the MIGRATE settings profile) must
        # be clamped down to 30s on the HTTP path.
        send_receive_timeout=300,
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client") as get_client,
        mock.patch("snuba.clickhouse.connect.get_pool_manager"),
    ):
        pool._get_client()

    _, kwargs = get_client.call_args
    assert kwargs["send_receive_timeout"] == 30
    assert kwargs["connect_timeout"] == 30


def test_send_receive_timeout_default_when_unset() -> None:
    import clickhouse_connect

    pool = ClickhouseConnectPool(
        host="host",
        http_port=8123,
        user="test",
        password="test",
        database="test",
        send_receive_timeout=None,
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client") as get_client,
        mock.patch("snuba.clickhouse.connect.get_pool_manager"),
    ):
        pool._get_client()

    _, kwargs = get_client.call_args
    assert kwargs["send_receive_timeout"] == 30


def test_pool_size_defaults_to_setting() -> None:
    import clickhouse_connect

    from snuba import settings

    pool = ClickhouseConnectPool(
        host="host", http_port=8123, user="test", password="test", database="test"
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client"),
        mock.patch("snuba.clickhouse.connect.get_pool_manager") as get_pool_manager,
    ):
        pool._get_client()

    _, kwargs = get_pool_manager.call_args
    assert kwargs["maxsize"] == settings.CLICKHOUSE_MAX_POOL_SIZE


@pytest.mark.redis_db
def test_pool_size_runtime_override() -> None:
    import clickhouse_connect

    from snuba import state

    state.set_config("clickhouse_connect_pool_size", 42)

    pool = ClickhouseConnectPool(
        host="host", http_port=8123, user="test", password="test", database="test"
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client"),
        mock.patch("snuba.clickhouse.connect.get_pool_manager") as get_pool_manager,
    ):
        pool._get_client()

    _, kwargs = get_pool_manager.call_args
    assert kwargs["maxsize"] == 42


def _make_selecting_pool() -> tuple[DriverSelectingPool, mock.Mock, mock.Mock]:
    native_pool = mock.Mock()
    native_pool.host = "host"
    native_pool.port = 9000
    native_pool.user = "test"
    native_pool.password = "test"
    native_pool.database = "test"
    connect_pool = mock.Mock()
    selecting = DriverSelectingPool(native_pool, connect_pool)
    return selecting, native_pool, connect_pool


@pytest.mark.redis_db
def test_selecting_pool_routes_to_native_by_default() -> None:
    selecting, native_pool, connect_pool = _make_selecting_pool()

    selecting.execute("SELECT 1")
    selecting.execute_robust("SELECT 2")

    assert native_pool.execute.call_count == 1
    assert native_pool.execute_robust.call_count == 1
    assert connect_pool.execute.call_count == 0
    assert connect_pool.execute_robust.call_count == 0


@pytest.mark.redis_db
def test_selecting_pool_routes_to_connect_when_enabled() -> None:
    from snuba import state

    state.set_config("use_clickhouse_connect_driver", 1)

    selecting, native_pool, connect_pool = _make_selecting_pool()

    selecting.execute("SELECT 1")
    selecting.execute_robust("SELECT 2")

    assert connect_pool.execute.call_count == 1
    assert connect_pool.execute_robust.call_count == 1
    assert native_pool.execute.call_count == 0
    assert native_pool.execute_robust.call_count == 0


@pytest.mark.redis_db
def test_selecting_pool_switches_at_runtime_without_recreating() -> None:
    from snuba import state

    selecting, native_pool, connect_pool = _make_selecting_pool()

    selecting.execute("native")
    state.set_config("use_clickhouse_connect_driver", 1)
    selecting.execute("connect")
    state.set_config("use_clickhouse_connect_driver", 0)
    selecting.execute("native again")

    # The same two underlying pools are reused throughout; only the routing
    # changes.
    assert native_pool.execute.call_count == 2
    assert connect_pool.execute.call_count == 1


def test_selecting_pool_close_closes_both() -> None:
    selecting, native_pool, connect_pool = _make_selecting_pool()

    selecting.close()

    assert native_pool.close.call_count == 1
    assert connect_pool.close.call_count == 1


@pytest.mark.redis_db
def test_selecting_pool_without_connect_pool_always_native() -> None:
    from snuba import state

    # No connect pool available (e.g. HTTP port unknown): queries must always
    # go to native, even when the runtime flag is on.
    state.set_config("use_clickhouse_connect_driver", 1)

    native_pool = mock.Mock()
    native_pool.host = "host"
    native_pool.port = 9000
    native_pool.user = "test"
    native_pool.password = "test"
    native_pool.database = "test"
    selecting = DriverSelectingPool(native_pool, None)

    selecting.execute("SELECT 1")
    selecting.close()  # must not raise despite the missing connect pool

    assert native_pool.execute.call_count == 1
    assert native_pool.close.call_count == 1
