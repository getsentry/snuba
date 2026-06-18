from typing import Any
from unittest import mock

import pytest

from snuba.clickhouse.connect import ClickhouseConnectPool
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


def test_timeouts_are_passed_through() -> None:
    import clickhouse_connect

    # The per-profile timeout is honored as-is (no capping), the same way the
    # native driver uses it. A large timeout (e.g. from MIGRATE) is preserved.
    pool = ClickhouseConnectPool(
        host="host",
        user="test",
        password="test",
        database="test",
        connect_timeout=60,
        send_receive_timeout=300000,
    )

    with (
        mock.patch.object(clickhouse_connect, "get_client") as get_client,
        mock.patch("snuba.clickhouse.connect.get_pool_manager"),
    ):
        pool._get_client()

    _, kwargs = get_client.call_args
    assert kwargs["send_receive_timeout"] == 300000
    assert kwargs["connect_timeout"] == 60


def test_send_receive_timeout_default_when_unset() -> None:
    import clickhouse_connect

    pool = ClickhouseConnectPool(
        host="host",
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
    assert kwargs["send_receive_timeout"] == 300


def test_read_query_client_settings_use_30s_timeout() -> None:
    # Read queries (the QUERY profile) get a 30s timeout on both drivers.
    from snuba.clusters.cluster import ClickhouseClientSettings

    assert ClickhouseClientSettings.QUERY.value.timeout == 30


def test_pool_size_defaults_to_setting() -> None:
    import clickhouse_connect

    from snuba import settings

    pool = ClickhouseConnectPool(host="host", user="test", password="test", database="test")

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

    pool = ClickhouseConnectPool(host="host", user="test", password="test", database="test")

    with (
        mock.patch.object(clickhouse_connect, "get_client"),
        mock.patch("snuba.clickhouse.connect.get_pool_manager") as get_pool_manager,
    ):
        pool._get_client()

    _, kwargs = get_pool_manager.call_args
    assert kwargs["maxsize"] == 42


def test_clickhouse_reader_wraps_connect_pool() -> None:
    # The single driver-agnostic ClickhouseReader wraps the abstract pool, so it
    # works with the connect pool just like the native one.
    from snuba.clickhouse.native import ClickhouseReader

    pool = _make_pool(mock.Mock())
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)
    assert isinstance(reader, ClickhouseReader)


def test_with_totals_handled_over_http() -> None:
    # WITH TOTALS works on the HTTP driver through clickhouse-connect's own
    # parsing: its Native-format reader concatenates every response block,
    # including the trailing totals block, into result_set, so the totals row
    # arrives last — exactly what the driver-agnostic ClickhouseReader expects
    # when it pops the last row as "totals". No native-driver-specific handling
    # is involved.
    from snuba.clickhouse.native import ClickhouseReader

    class FakeFormattedQuery:
        def get_sql(self) -> str:
            return "SELECT project_id, count() FROM t GROUP BY project_id WITH TOTALS"

    client = mock.Mock()
    # Two data rows followed by the totals row, the way clickhouse-connect
    # surfaces a WITH TOTALS response over HTTP.
    client.query.return_value = FakeQueryResult(
        result_set=[[1, 10], [2, 20], [0, 30]],
        column_names=("project_id", "count()"),
        column_types=(FakeColumnType("UInt64"), FakeColumnType("UInt64")),
    )

    pool = _make_pool(client)
    reader = ClickhouseReader(cache_partition_id=None, client=pool, query_settings_prefix=None)

    result = reader.execute(FakeFormattedQuery(), with_totals=True)

    # The trailing row is split out as totals; only the real rows remain in data.
    assert result["data"] == [
        {"project_id": 1, "count()": 10},
        {"project_id": 2, "count()": 20},
    ]
    assert result["totals"] == {"project_id": 0, "count()": 30}
