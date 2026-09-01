from __future__ import annotations

import importlib
from typing import Any
from unittest.mock import Mock

import pytest

from snuba.clickhouse.pool import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import operations as migration_operations
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.migration_utilities import strip_sample_by_clause
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status

querylog_0008 = importlib.import_module("snuba.snuba_migrations.querylog.0008_drop_uuid_sample_by")
DropUuidSampleBy = querylog_0008.DropUuidSampleBy

SHOW_CREATE_MULTILINE = """CREATE TABLE default.querylog_local
(
    `request_id` UUID CODEC(NONE),
    `request_body` String CODEC(LZ4HC(0)),
    `referrer` LowCardinality(String),
    `dataset` LowCardinality(String),
    `timestamp` DateTime CODEC(T64, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toMonday(timestamp)
ORDER BY (dataset, referrer, toStartOfDay(timestamp), request_id)
SAMPLE BY request_id
TTL timestamp + toIntervalDay(30)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 10000000, ttl_only_drop_parts = 1"""

SHOW_CREATE_INLINE = (
    "CREATE TABLE default.querylog_local (`request_id` UUID, `timestamp` DateTime) "
    "ENGINE = MergeTree() ORDER BY (toStartOfDay(timestamp), request_id) "
    "PARTITION BY toMonday(timestamp) SAMPLE BY request_id "
    "SETTINGS index_granularity = 8192"
)

SHOW_CREATE_REPLICATED = """CREATE TABLE default.querylog_local
(
    `request_id` UUID
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/querylog/{shard}/default/querylog_local', '{replica}')
ORDER BY (toStartOfDay(timestamp), request_id)
SAMPLE BY request_id
SETTINGS index_granularity = 8192"""

SHOW_CREATE_NO_SAMPLE = """CREATE TABLE default.querylog_local
(
    `request_id` UUID
)
ENGINE = MergeTree
ORDER BY (toStartOfDay(timestamp), request_id)
SETTINGS index_granularity = 8192"""


@pytest.mark.parametrize(
    "create_sql",
    [SHOW_CREATE_MULTILINE, SHOW_CREATE_INLINE, SHOW_CREATE_REPLICATED],
    ids=["multiline", "inline", "replicated"],
)
def test_strip_sample_by_clause(create_sql: str) -> None:
    stripped = strip_sample_by_clause(create_sql)
    assert "SAMPLE BY" not in stripped.upper()
    assert "ORDER BY" in stripped


def test_strip_sample_by_preserves_statement_without_clause() -> None:
    assert strip_sample_by_clause(SHOW_CREATE_NO_SAMPLE) == SHOW_CREATE_NO_SAMPLE


def test_strip_sample_by_function_expression() -> None:
    statement = (
        "CREATE TABLE t (id UUID) ENGINE = MergeTree "
        "ORDER BY cityHash64(id) SAMPLE BY cityHash64(id) TTL ts + INTERVAL 1 DAY"
    )
    stripped = strip_sample_by_clause(statement)
    assert "SAMPLE BY" not in stripped.upper()
    assert "ORDER BY cityHash64(id)" in stripped
    assert "TTL ts + INTERVAL 1 DAY" in stripped


class _FakePool:
    def __init__(
        self,
        *,
        sampling_key: str = "",
        missing_table: bool = False,
    ) -> None:
        self.sampling_key = sampling_key
        self.missing_table = missing_table
        self.commands: list[str] = []

    def execute(self, query: str, *args: Any, **kwargs: Any) -> ClickhouseResult:
        if query.startswith("SELECT sampling_key"):
            if self.missing_table:
                return ClickhouseResult(results=[])
            return ClickhouseResult(results=[(self.sampling_key,)])
        return ClickhouseResult(results=[])

    def command(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self.commands.append(sql)


def _execute(sampling_key: str, *, missing_table: bool = False) -> _FakePool:
    pool = _FakePool(sampling_key=sampling_key, missing_table=missing_table)
    node = Mock()
    mock_cluster = Mock()
    mock_cluster.get_local_nodes.return_value = [node]
    mock_cluster.get_database.return_value = "default"
    mock_cluster.is_single_node.return_value = True
    mock_cluster.get_node_connection.return_value = pool
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(querylog_0008, "get_cluster", lambda storage_set: mock_cluster)
        mp.setattr(migration_operations, "get_cluster", lambda storage_set: mock_cluster)
        DropUuidSampleBy().execute()
    return pool


@pytest.mark.parametrize("sampling_key", ["", "cityHash64(request_id)"])
def test_forwards_ops_noop(sampling_key: str) -> None:
    assert _execute(sampling_key).commands == []


def test_forwards_ops_noop_when_table_missing() -> None:
    assert _execute("", missing_table=True).commands == []


def test_forwards_ops_removes_sample_by_in_place() -> None:
    pool = _execute("request_id")
    assert len(pool.commands) == 1
    sql = pool.commands[0]
    assert sql.startswith("ALTER TABLE querylog_local")
    assert "REMOVE SAMPLE BY" in sql


def test_migration_is_registered() -> None:
    migrations = get_group_loader(MigrationGroup.QUERYLOG).get_migrations()
    assert "0008_drop_uuid_sample_by" in migrations
    loaded = get_group_loader(MigrationGroup.QUERYLOG).load_migration("0008_drop_uuid_sample_by")
    assert isinstance(loaded, ClickhouseNodeMigration)
    assert loaded.blocking is False
    assert loaded.backwards_ops() == []


@pytest.mark.custom_clickhouse_db
def test_querylog_migration_noop_on_fresh_schema() -> None:
    cluster = get_cluster(StorageSetKey.QUERYLOG)
    if not cluster.is_single_node():
        return

    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"), force=True)
    for migration_id in get_group_loader(MigrationGroup.QUERYLOG).get_migrations():
        runner.run_migration(
            MigrationKey(MigrationGroup.QUERYLOG, migration_id),
            force=True,
        )

    assert (
        runner.get_status(MigrationKey(MigrationGroup.QUERYLOG, "0008_drop_uuid_sample_by"))[0]
        == Status.COMPLETED
    )

    connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    database = cluster.get_database()
    ((sampling_key,),) = connection.execute(
        "SELECT sampling_key FROM system.tables "
        f"WHERE name = 'querylog_local' AND database = '{database}'"
    ).results
    assert sampling_key == ""
    ((create_sql,),) = connection.execute(f"SHOW CREATE TABLE {database}.querylog_local").results
    assert "SAMPLE BY" not in create_sql.upper()
