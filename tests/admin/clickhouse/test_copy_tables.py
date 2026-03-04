import os

import pytest

from snuba.admin.clickhouse.common import _get_storage, get_clusterless_node_connection
from snuba.admin.clickhouse.copy_tables import (
    WorkloadStatement,
    _topological_sort_workloads,
    copy_tables,
    get_create_table_statements,
    verify_tables_on_replicas,
)
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.migrations import table_engines
from snuba.migrations.groups import MigrationGroup

OUTCOMES_DAILY_TABLE = """
CREATE TABLE IF NOT EXISTS {db}.outcomes_daily_local_v2 ON CLUSTER 'test_cluster'
(
    `org_id` UInt64,
    `project_id` UInt64,
    `key_id` UInt64,
    `timestamp` DateTime,
    `outcome` UInt8,
    `reason` LowCardinality(String),
    `category` UInt8,
    `quantity` UInt64,
    `times_seen` UInt64
)
ENGINE = {engine}
PARTITION BY toStartOfMonth(timestamp)
ORDER BY (org_id, project_id, key_id, outcome, reason, timestamp, category)
TTL timestamp + toIntervalMonth(13)
SETTINGS index_granularity = 8192
"""

OUTCOMES_DAILY_MV = """
CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.outcomes_mv_daily_local_v2 ON CLUSTER 'test_cluster' TO {db}.outcomes_daily_local_v2
(
    `org_id` UInt64,
    `project_id` UInt64,
    `key_id` UInt64,
    `timestamp` DateTime,
    `outcome` UInt8,
    `reason` String,
    `category` UInt8,
    `quantity` UInt64,
    `times_seen` UInt64
)
AS SELECT
    org_id,
    project_id,
    ifNull(key_id, 0) AS key_id,
    toStartOfDay(timestamp) AS timestamp,
    outcome,
    ifNull(reason, 'none') AS reason,
    category,
    count() AS times_seen,
    sum(quantity) AS quantity
FROM {db}.outcomes_raw_local
GROUP BY
    org_id,
    project_id,
    key_id,
    timestamp,
    outcome,
    reason,
    category
"""

TABLE_DATA = [
    ("outcomes_daily_local_v2", "outcomes_daily", OUTCOMES_DAILY_TABLE, True),
    ("outcomes_mv_daily_local_v2", "outcomes_daily", OUTCOMES_DAILY_MV, False),
]


def run_migrations() -> None:
    from snuba.migrations.runner import Runner

    migration_group = MigrationGroup("outcomes")
    Runner().run_all(force=True, group=migration_group)


@pytest.mark.parametrize("table, storage_name, statement, is_mergetree", TABLE_DATA)
@pytest.mark.redis_db
@pytest.mark.custom_clickhouse_db
def test_get_table_statements(
    table: str, storage_name: str, statement: str, is_mergetree: bool
) -> None:
    """
    The create statements should have IF EXISTS added, as well
    as the ON CLUSTER clause.
    """
    run_migrations()
    host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
    settings = ClickhouseClientSettings.QUERY
    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database_name = cluster.get_database()
    if cluster.is_single_node():
        engine = "SummingMergeTree"
    else:
        engine = table_engines.SummingMergeTree(
            storage_set=storage.get_storage_set_key(),
            order_by="",
        )._get_engine_type(cluster, table)
    table_statements = get_create_table_statements(
        tables=[table],
        source_connection=get_clusterless_node_connection(
            host, 9000, storage_name, client_settings=settings
        ),
        source_database=database_name,
        cluster_name="test_cluster",
    )
    ts = table_statements[0]
    assert ts.is_mergetree == is_mergetree
    assert ts.statement == statement.format(db=database_name, engine=engine).strip()


@pytest.mark.redis_db
@pytest.mark.custom_clickhouse_db
def test_create_tables_order() -> None:
    """
    Make sure the order in which we create tables is correct.
    All local (mergetree) tables should be created
    before Merge (discover) & Materialized Views (non mergetree)
    """
    run_migrations()
    host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
    expected_local_tables = [
        "migrations_local",
        "outcomes_daily_local_v2",
        "outcomes_hourly_local",
        "outcomes_raw_local",
    ]
    expected_non_local_tables = [
        "migrations_dist",
        "outcomes_daily_dist_v2",
        "outcomes_hourly_dist",
        "outcomes_mv_daily_local_v2",
        "outcomes_mv_hourly_local",
        "outcomes_raw_dist",
    ]

    results = copy_tables(source_host=host, storage_name="outcomes_raw", dry_run=True)
    all_tables = str(results["tables"])

    local_tables = all_tables.split(",")[:4]
    non_local_tables = all_tables.split(",")[4:]
    assert local_tables == expected_local_tables
    assert all(table in expected_non_local_tables for table in non_local_tables)


@pytest.mark.redis_db
@pytest.mark.custom_clickhouse_db
def test_verify_tables_on_replicas() -> None:
    """
    Test that verify_tables_on_replicas correctly identifies missing tables.
    """
    run_migrations()
    host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
    settings = ClickhouseClientSettings.QUERY
    storage = _get_storage("outcomes_raw")
    cluster = storage.get_cluster()
    database_name = cluster.get_database()
    cluster_name = None if cluster.is_single_node() else cluster.get_clickhouse_cluster_name()

    connection = get_clusterless_node_connection(
        host, 9000, "outcomes_raw", client_settings=settings
    )

    # Test with table that exist, all should be verified
    existing_tables = ["outcomes_raw_local"]
    missing_hosts, verified_count = verify_tables_on_replicas(
        connection, cluster_name, database_name, existing_tables
    )
    assert verified_count > 0
    assert missing_hosts == {}

    # Test with a non-existent table, none should be verified
    nonexistent_tables = ["nonexistent_table_xyz"]
    missing_hosts, verified_count = verify_tables_on_replicas(
        connection, cluster_name, database_name, nonexistent_tables
    )

    assert verified_count == 0
    assert ["nonexistent_table_xyz"] in missing_hosts.values()


def test_topological_sort_workloads_empty() -> None:
    """Test that an empty list returns an empty list."""
    result = _topological_sort_workloads([])
    assert result == []


def test_topological_sort_workloads_single() -> None:
    """Test sorting a single workload with no parent."""
    workloads = [WorkloadStatement(name="all", parent="", statement="CREATE WORKLOAD all")]
    result = _topological_sort_workloads(workloads)
    assert len(result) == 1
    assert result[0].name == "all"


def test_topological_sort_workloads_hierarchy() -> None:
    """Test that parents come before children in the sorted output."""
    workloads = [
        WorkloadStatement(
            name="low_priority_deletes",
            parent="all",
            statement="CREATE WORKLOAD low_priority_deletes IN all",
        ),
        WorkloadStatement(
            name="all",
            parent="",
            statement="CREATE WORKLOAD all",
        ),
        WorkloadStatement(
            name="sub_workload",
            parent="low_priority_deletes",
            statement="CREATE WORKLOAD sub_workload IN low_priority_deletes",
        ),
    ]
    result = _topological_sort_workloads(workloads)
    names = [w.name for w in result]

    # "all" must come before "low_priority_deletes"
    assert names.index("all") < names.index("low_priority_deletes")
    # "low_priority_deletes" must come before "sub_workload"
    assert names.index("low_priority_deletes") < names.index("sub_workload")


def test_topological_sort_workloads_multiple_roots() -> None:
    """Test sorting workloads with multiple roots (no parent)."""
    workloads = [
        WorkloadStatement(
            name="child_a", parent="root_a", statement="CREATE WORKLOAD child_a IN root_a"
        ),
        WorkloadStatement(name="root_a", parent="", statement="CREATE WORKLOAD root_a"),
        WorkloadStatement(name="root_b", parent="", statement="CREATE WORKLOAD root_b"),
        WorkloadStatement(
            name="child_b", parent="root_b", statement="CREATE WORKLOAD child_b IN root_b"
        ),
    ]
    result = _topological_sort_workloads(workloads)
    names = [w.name for w in result]

    # Each parent must come before its child
    assert names.index("root_a") < names.index("child_a")
    assert names.index("root_b") < names.index("child_b")


def test_topological_sort_workloads_external_parent() -> None:
    """Test workload with parent not in the list (treated as root)."""
    workloads = [
        WorkloadStatement(
            name="child",
            parent="external_parent",
            statement="CREATE WORKLOAD child IN external_parent",
        ),
    ]
    result = _topological_sort_workloads(workloads)
    assert len(result) == 1
    assert result[0].name == "child"
