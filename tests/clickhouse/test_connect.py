from typing import Any
from unittest import mock

import pytest

from snuba import state
from snuba.clickhouse.connect import (
    TOO_MANY_SIMULTANEOUS_QUERIES,
    ClickhouseConnectPool,
)
from snuba.clickhouse.errors import ClickhouseError


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


@pytest.mark.redis_db
def test_too_many_simultaneous_queries_retry() -> None:
    from clickhouse_connect.driver.exceptions import DatabaseError

    state.set_config("simultaneous_queries_sleep_seconds", 0.01)

    client = mock.Mock()
    client.query.side_effect = DatabaseError("too many", code=TOO_MANY_SIMULTANEOUS_QUERIES)

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute("SELECT 1")

    # Initial attempt + one retry once the concurrency limit is hit.
    assert client.query.call_count == 2


@pytest.mark.redis_db
def test_operational_error_retries() -> None:
    from clickhouse_connect.driver.exceptions import OperationalError

    client = mock.Mock()
    client.query.side_effect = OperationalError("connection refused")

    pool = _make_pool(client)
    with pytest.raises(ClickhouseError):
        pool.execute("SELECT 1", retryable=True)

    assert client.query.call_count == 3
