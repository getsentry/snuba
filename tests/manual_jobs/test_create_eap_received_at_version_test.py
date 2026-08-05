from unittest.mock import Mock, call

import pytest

from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.create_eap_received_at_version_test import (
    CreateEAPReceivedAtVersionTest,
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


def _build_job(monkeypatch: pytest.MonkeyPatch) -> CreateEAPReceivedAtVersionTest:
    monkeypatch.setattr("snuba.manual_jobs._set_job_type", Mock())
    return CreateEAPReceivedAtVersionTest(
        JobSpec(
            job_id="create-eap-received-at-version-test",
            job_type="CreateEAPReceivedAtVersionTest",
        )
    )


def _build_cluster(*, single_node: bool) -> Mock:
    cluster = Mock()
    cluster.is_single_node.return_value = single_node
    cluster.get_clickhouse_cluster_name.return_value = "eap_cluster"
    cluster.get_database.return_value = "default"
    return cluster


def test_job_requires_manifest() -> None:
    assert not CreateEAPReceivedAtVersionTest.allow_adhoc_run


def test_multi_node_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch)
    cluster = _build_cluster(single_node=False)

    assert job._add_received_at_query(cluster) == (
        "ALTER TABLE eap_items_1_local ON CLUSTER 'eap_cluster' "
        "ADD COLUMN IF NOT EXISTS received_at UInt64"
    )

    create_table = job._create_table_query(cluster, _COLUMNS)
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

    create_view = job._create_view_query(cluster, _COLUMNS)
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
    assert create_view.endswith("WHERE (cityHash64(item_id) % 8) = 0")


def test_single_node_uses_non_replicated_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch)
    cluster = _build_cluster(single_node=True)

    create_table = job._create_table_query(cluster, _COLUMNS)
    assert "ON CLUSTER" not in create_table
    assert "ENGINE = ReplacingMergeTree(received_at)" in create_table
    assert "ReplicatedReplacingMergeTree" not in create_table


def test_execute_uses_eap_local_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch)
    connection = Mock()
    connection.execute.side_effect = [
        ClickhouseResult(),
        ClickhouseResult(results=_COLUMNS),
        ClickhouseResult(),
        ClickhouseResult(),
    ]
    cluster = _build_cluster(single_node=False)
    cluster.get_local_nodes.return_value = ["local-node"]
    cluster.get_node_connection.return_value = connection
    monkeypatch.setattr(
        "snuba.manual_jobs.create_eap_received_at_version_test.get_cluster",
        Mock(return_value=cluster),
    )

    job.execute(Mock())

    cluster.get_node_connection.assert_called_once_with(
        ClickhouseClientSettings.MIGRATE, "local-node"
    )
    assert connection.execute.call_args_list[0] == call(query=job._add_received_at_query(cluster))
    assert connection.execute.call_args_list[1] == call(query=job._get_columns_query())
    assert connection.execute.call_args_list[2] == call(
        query=job._create_table_query(cluster, _COLUMNS)
    )
    assert connection.execute.call_args_list[3] == call(
        query=job._create_view_query(cluster, _COLUMNS)
    )


def test_rejects_empty_source_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch)

    with pytest.raises(AssertionError, match="has no columns"):
        job._create_table_query(_build_cluster(single_node=True), [])
