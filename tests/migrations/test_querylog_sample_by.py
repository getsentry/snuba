from __future__ import annotations

import importlib
from typing import Any
from unittest.mock import Mock

import pytest

from snuba.clickhouse.pool import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.migration_utilities import strip_sample_by_clause
from snuba.migrations.operations import DropTable, InsertIntoSelect, RenameTable, RunSql
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status

querylog_0008 = importlib.import_module("snuba.snuba_migrations.querylog.0008_drop_uuid_sample_by")
forwards_ops = querylog_0008._forwards_ops

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
        create_sql: str = "",
        existing_tables: set[str] | None = None,
    ) -> None:
        self.sampling_key = sampling_key
        self.create_sql = create_sql
        self.existing_tables = existing_tables or set()

    def execute(self, query: str, *args: Any, **kwargs: Any) -> ClickhouseResult:
        if query.startswith("SELECT sampling_key"):
            return ClickhouseResult(results=[(self.sampling_key,)])
        if query.startswith("SHOW CREATE TABLE"):
            return ClickhouseResult(results=[(self.create_sql,)])
        if query.startswith("EXISTS TABLE"):
            table_name = query.split()[-1].rstrip(";")
            return ClickhouseResult(results=[(1 if table_name in self.existing_tables else 0,)])
        return ClickhouseResult(results=[])


def _ops_for(sampling_key: str, create_sql: str) -> list[Any]:
    mock_cluster = Mock()
    mock_cluster.get_local_nodes.return_value = [Mock()]
    mock_cluster.get_database.return_value = "default"
    mock_cluster.get_node_connection.return_value = _FakePool(
        sampling_key=sampling_key, create_sql=create_sql
    )
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(querylog_0008, "get_cluster", lambda storage_set: mock_cluster)
        return list(forwards_ops())


def _backwards_ops_for(existing_tables: set[str]) -> list[Any]:
    mock_cluster = Mock()
    mock_cluster.get_local_nodes.return_value = [Mock()]
    mock_cluster.get_database.return_value = "default"
    mock_cluster.get_node_connection.return_value = _FakePool(existing_tables=existing_tables)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(querylog_0008, "get_cluster", lambda storage_set: mock_cluster)
        return list(querylog_0008._backwards_ops())


@pytest.mark.parametrize("sampling_key", ["", "cityHash64(request_id)"])
def test_forwards_ops_noop(sampling_key: str) -> None:
    assert _ops_for(sampling_key, SHOW_CREATE_NO_SAMPLE) == []


def test_forwards_ops_rebuilds_when_sample_by_is_request_id() -> None:
    ops = _ops_for("request_id", SHOW_CREATE_MULTILINE)
    assert [type(op) for op in ops] == [
        RunSql,
        InsertIntoSelect,
        RenameTable,
        RenameTable,
        DropTable,
    ]

    create_sql = ops[0].format_sql()
    assert "SAMPLE BY" not in create_sql.upper()
    assert "querylog_local_new" in create_sql

    assert "INSERT INTO querylog_local_new" in ops[1].format_sql()
    assert ops[2].format_sql().startswith("RENAME TABLE querylog_local TO querylog_local_old")
    assert ops[3].format_sql().startswith("RENAME TABLE querylog_local_new TO querylog_local")
    assert "DROP TABLE IF EXISTS querylog_local_old" in ops[4].format_sql()


def test_backwards_ops_restores_old_table_when_local_missing() -> None:
    ops = _backwards_ops_for({"querylog_local_old", "querylog_local_new"})
    assert [type(op) for op in ops] == [RenameTable, DropTable]
    assert ops[0].format_sql().startswith("RENAME TABLE querylog_local_old TO querylog_local")
    assert "DROP TABLE IF EXISTS querylog_local_new" in ops[1].format_sql()


def test_backwards_ops_does_not_drop_old_when_it_is_the_only_copy() -> None:
    ops = _backwards_ops_for({"querylog_local_old"})
    assert [type(op) for op in ops] == [RenameTable]
    assert ops[0].format_sql().startswith("RENAME TABLE querylog_local_old TO querylog_local")


def test_backwards_ops_drops_temps_when_local_exists() -> None:
    ops = _backwards_ops_for({"querylog_local", "querylog_local_new", "querylog_local_old"})
    assert [type(op) for op in ops] == [DropTable, DropTable]
    assert "DROP TABLE IF EXISTS querylog_local_new" in ops[0].format_sql()
    assert "DROP TABLE IF EXISTS querylog_local_old" in ops[1].format_sql()


def test_migration_is_registered() -> None:
    migrations = get_group_loader(MigrationGroup.QUERYLOG).get_migrations()
    assert "0008_drop_uuid_sample_by" in migrations
    loaded = get_group_loader(MigrationGroup.QUERYLOG).load_migration("0008_drop_uuid_sample_by")
    assert loaded.blocking is True


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
