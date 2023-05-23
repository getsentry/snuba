from unittest.mock import call, patch

import pytest
from clickhouse_driver import Client, errors

from snuba.clickhouse.columns import Array
from snuba.clickhouse.columns import SchemaModifiers as Modifier
from snuba.clickhouse.columns import UInt
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey


def test_flattened() -> None:
    columns = (
        get_writable_storage(StorageKey("errors"))
        .get_table_writer()
        .get_schema()
        .get_columns()
    )

    # columns = enforce_table_writer(self.dataset).get_schema().get_columns()
    assert columns["group_id"].type == UInt(64)
    assert columns["group_id"].name == "group_id"
    assert columns["group_id"].base_name is None
    assert columns["group_id"].flattened == "group_id"

    assert columns["exception_frames.in_app"].type == Array(
        UInt(8, Modifier(nullable=True))
    )
    assert columns["exception_frames.in_app"].name == "in_app"
    assert columns["exception_frames.in_app"].base_name == "exception_frames"
    assert columns["exception_frames.in_app"].flattened == "exception_frames.in_app"


@patch("snuba.clickhouse.native.Client")
def test_reconnect(FakeClient: Client) -> None:
    # If the connection NetworkErrors a first time, make sure we call it a second time.
    FakeClient.return_value.execute.side_effect = [
        errors.NetworkError,
        '{"data": "to my face"}',
    ]
    cp = ClickhousePool("0:0:0:0", 9000, "default", "", "default")
    cp.execute("SHOW TABLES")
    assert FakeClient.return_value.execute.mock_calls == [
        call(
            "SHOW TABLES",
            params=None,
            with_column_types=False,
            query_id=None,
            settings=None,
            types_check=False,
            columnar=False,
        ),
        call(
            "SHOW TABLES",
            params=None,
            with_column_types=False,
            query_id=None,
            settings=None,
            types_check=False,
            columnar=False,
        ),
    ]


@pytest.mark.clickhouse_db
def test_capture_trace() -> None:
    storage = get_storage(StorageKey.ERRORS)
    clickhouse = storage.get_cluster().get_query_connection(
        ClickhouseClientSettings.QUERY
    )

    data = clickhouse.execute(
        "SELECT count() FROM errors_local", with_column_types=True, capture_trace=True
    )
    assert data.results == [(0,)]
    assert data.meta == [("count()", "UInt64")]
    assert data.trace_output != ""
    assert data.profile is not None
    assert data.profile["elapsed"] > 0
    assert data.profile["bytes"] > 0
    assert data.profile["rows"] > 0
    assert data.profile["blocks"] > 0
