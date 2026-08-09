from unittest.mock import Mock, call

import pytest

from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.manual_jobs import Job, JobSpec
from snuba.manual_jobs.create_eap_received_at_version_test import (
    AddEAPReceivedAtColumn,
    CreateEAPReceivedAtVersionTestMaterializedView,
    CreateEAPReceivedAtVersionTestTable,
    _add_received_at_query,
    _create_table_query,
    _create_view_query,
)

_COLUMNS = [
    ("organization_id", "UInt64", "", "", "", "", ""),
    ("project_id", "UInt64", "", "", "", "", ""),
    ("item_type", "UInt8", "", "", "", "", ""),
    ("timestamp", "DateTime", "", "", "event time", "DoubleDelta, ZSTD(1)", ""),
    ("trace_id", "UUID", "", "", "", "", ""),
    ("item_id", "UInt128", "", "", "", "", ""),
    ("sampling_weight", "UInt64", "", "", "", "ZSTD(1)", ""),
    ("sampling_factor", "Float64", "", "", "", "ZSTD(1)", ""),
    ("retention_days", "UInt16", "DEFAULT", "30", "", "T64, ZSTD(1)", ""),
    ("client_sample_rate", "Float64", "", "", "", "", ""),
    ("server_sample_rate", "Float64", "", "", "", "", ""),
    ("attributes_string_0", "Map(String, String)", "", "", "", "ZSTD(1)", ""),
]
_DESTINATION_COLUMNS = [*_COLUMNS, ("received_at", "UInt64", "", "", "", "", "")]


def _build_job(monkeypatch: pytest.MonkeyPatch, job_type: type[Job]) -> Job:
    monkeypatch.setattr("snuba.manual_jobs._set_job_type", Mock())
    return job_type(
        JobSpec(
            job_id="eap-received-at-version-test",
            job_type=job_type.__name__,
        )
    )


def _build_cluster(*, single_node: bool) -> Mock:
    cluster = Mock()
    cluster.is_single_node.return_value = single_node
    cluster.get_clickhouse_cluster_name.return_value = "eap_cluster"
    cluster.get_clickhouse_distributed_cluster_name.return_value = "eap_dist_cluster"
    cluster.get_database.return_value = "default"
    cluster.get_local_nodes.return_value = ["local-node"]
    cluster.get_distributed_nodes.return_value = ["distributed-node"]
    return cluster


def _patch_cluster(monkeypatch: pytest.MonkeyPatch, connection: Mock) -> Mock:
    cluster = _build_cluster(single_node=False)
    cluster.get_node_connection.return_value = connection
    monkeypatch.setattr(
        "snuba.manual_jobs.create_eap_received_at_version_test.get_cluster",
        Mock(return_value=cluster),
    )
    return cluster


@pytest.mark.parametrize(
    "job_type",
    [
        AddEAPReceivedAtColumn,
        CreateEAPReceivedAtVersionTestTable,
        CreateEAPReceivedAtVersionTestMaterializedView,
    ],
)
def test_jobs_require_manifest(job_type: type[Job]) -> None:
    assert not job_type.allow_adhoc_run


def test_multi_node_queries() -> None:
    cluster = _build_cluster(single_node=False)

    assert _add_received_at_query(cluster, "eap_items_1_local") == (
        "ALTER TABLE eap_items_1_local ON CLUSTER 'eap_cluster' "
        "ADD COLUMN IF NOT EXISTS received_at UInt64"
    )
    assert _add_received_at_query(cluster, "eap_items_1_dist", distributed=True) == (
        "ALTER TABLE eap_items_1_dist ON CLUSTER 'eap_dist_cluster' "
        "ADD COLUMN IF NOT EXISTS received_at UInt64"
    )

    create_table = _create_table_query(cluster, _COLUMNS)
    assert create_table.startswith(
        "CREATE TABLE IF NOT EXISTS "
        "eap_items_1_downsample_8_timestamp_versioned_test_local "
        "ON CLUSTER 'eap_cluster' (organization_id UInt64"
    )
    assert "timestamp DateTime COMMENT 'event time' CODEC(DoubleDelta, ZSTD(1))" in create_table
    assert "retention_days UInt16 DEFAULT 30 CODEC(T64, ZSTD(1))" in create_table
    assert (
        "attributes_string_0 Map(String, String) CODEC(ZSTD(1)), received_at UInt64" in create_table
    )
    assert (
        "ReplicatedReplacingMergeTree("
        "'/clickhouse/tables/events_analytics_platform/{shard}/default/"
        "eap_items_1_downsample_8_timestamp_versioned_test_local', "
        "'{replica}', received_at)"
    ) in create_table
    assert "PARTITION BY (retention_days, toMonday(timestamp))" in create_table
    assert "TTL timestamp + toIntervalDay(retention_days)" in create_table
    assert (
        "SETTINGS index_granularity=8192, enable_block_number_column=1, "
        "enable_block_offset_column=1"
    ) in create_table

    create_view = _create_view_query(cluster, _DESTINATION_COLUMNS)
    assert create_view.startswith(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "eap_items_1_downsample_8_timestamp_versioned_test_mv "
        "ON CLUSTER 'eap_cluster' TO "
        "eap_items_1_downsample_8_timestamp_versioned_test_local AS SELECT "
    )
    assert "sampling_weight * 8 AS sampling_weight" in create_view
    assert "sampling_factor / 8 AS sampling_factor" in create_view
    assert "downsampled_retention_days AS retention_days" in create_view
    assert "server_sample_rate / 8 AS server_sample_rate" in create_view
    assert "attributes_string_0, received_at FROM eap_items_1_local" in create_view
    assert create_view.endswith("WHERE received_at != 0 AND (cityHash64(item_id) % 8) = 0")


def test_single_node_uses_non_replicated_engine() -> None:
    create_table = _create_table_query(_build_cluster(single_node=True), _COLUMNS)

    assert "ON CLUSTER" not in create_table
    assert "ENGINE = ReplacingMergeTree(received_at)" in create_table
    assert "ReplicatedReplacingMergeTree" not in create_table


def test_add_column_job_alters_local_and_distributed_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _build_job(monkeypatch, AddEAPReceivedAtColumn)
    local_connection = Mock()
    distributed_connection = Mock()
    cluster = _patch_cluster(monkeypatch, local_connection)
    cluster.get_node_connection.side_effect = [local_connection, distributed_connection]

    job.execute(Mock())

    assert cluster.get_node_connection.call_args_list == [
        call(ClickhouseClientSettings.MIGRATE, "local-node"),
        call(ClickhouseClientSettings.MIGRATE, "distributed-node"),
    ]
    local_connection.execute.assert_called_once_with(
        query=_add_received_at_query(cluster, "eap_items_1_local")
    )
    distributed_connection.execute.assert_called_once_with(
        query=_add_received_at_query(cluster, "eap_items_1_dist", distributed=True)
    )


def test_add_column_job_skips_dist_table_on_single_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _build_job(monkeypatch, AddEAPReceivedAtColumn)
    connection = Mock()
    cluster = _build_cluster(single_node=True)
    cluster.get_node_connection.return_value = connection
    monkeypatch.setattr(
        "snuba.manual_jobs.create_eap_received_at_version_test.get_cluster",
        Mock(return_value=cluster),
    )

    job.execute(Mock())

    cluster.get_node_connection.assert_called_once_with(
        ClickhouseClientSettings.MIGRATE, "local-node"
    )
    connection.execute.assert_called_once_with(
        query=_add_received_at_query(cluster, "eap_items_1_local")
    )


def test_create_table_job_does_not_create_view(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch, CreateEAPReceivedAtVersionTestTable)
    connection = Mock()
    connection.execute.side_effect = [ClickhouseResult(results=_COLUMNS), ClickhouseResult()]
    cluster = _patch_cluster(monkeypatch, connection)

    job.execute(Mock())

    assert connection.execute.call_args_list == [
        call(
            query="DESCRIBE TABLE eap_items_1_downsample_8_local "
            "SETTINGS describe_include_subcolumns = 0"
        ),
        call(query=_create_table_query(cluster, _COLUMNS)),
    ]


def test_create_view_job_uses_destination_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch, CreateEAPReceivedAtVersionTestMaterializedView)
    connection = Mock()
    connection.execute.side_effect = [
        ClickhouseResult(results=_DESTINATION_COLUMNS),
        ClickhouseResult(),
    ]
    cluster = _patch_cluster(monkeypatch, connection)

    job.execute(Mock())

    assert connection.execute.call_args_list == [
        call(
            query="DESCRIBE TABLE eap_items_1_downsample_8_timestamp_versioned_test_local "
            "SETTINGS describe_include_subcolumns = 0"
        ),
        call(query=_create_view_query(cluster, _DESTINATION_COLUMNS)),
    ]


def test_rejects_empty_schemas() -> None:
    cluster = _build_cluster(single_node=True)

    with pytest.raises(AssertionError, match="downsample_8_local has no columns"):
        _create_table_query(cluster, [])
    with pytest.raises(AssertionError, match="versioned_test_local has no columns"):
        _create_view_query(cluster, [])
