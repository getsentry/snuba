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
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status

querylog_0008 = importlib.import_module("snuba.snuba_migrations.querylog.0008_drop_uuid_sample_by")

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
    "original, unexpected_fragment",
    [
        pytest.param(SHOW_CREATE_MULTILINE, "SAMPLE BY request_id", id="multiline"),
        pytest.param(SHOW_CREATE_INLINE, "SAMPLE BY request_id", id="inline"),
        pytest.param(SHOW_CREATE_REPLICATED, "SAMPLE BY request_id", id="replicated"),
    ],
)
def test_strip_sample_by_clause(original: str, unexpected_fragment: str) -> None:
    stripped = strip_sample_by_clause(original)
    assert "SAMPLE BY" not in stripped.upper()
    assert unexpected_fragment not in stripped
    assert "ORDER BY" in stripped
    assert "querylog_local" in stripped


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
        sampling_key: str,
        create_sql: str,
        row_count: int = 0,
    ) -> None:
        self.sampling_key = sampling_key
        self.create_sql = create_sql
        self.row_count = row_count
        self.executed: list[str] = []
        self.commands: list[str] = []

    def execute(self, query: str, *args: Any, **kwargs: Any) -> ClickhouseResult:
        self.executed.append(query)
        if query.startswith("SELECT sampling_key"):
            return ClickhouseResult(results=[(self.sampling_key,)])
        if query.startswith("SHOW CREATE TABLE"):
            return ClickhouseResult(results=[(self.create_sql,)])
        if query.startswith("SELECT count()"):
            return ClickhouseResult(results=[(self.row_count,)])
        return ClickhouseResult(results=[])

    def command(self, statement: str, *args: Any, **kwargs: Any) -> ClickhouseResult:
        self.commands.append(statement)
        return ClickhouseResult(results=[])


def test_update_querylog_table_noop_without_sample_by() -> None:
    pool = _FakePool(sampling_key="", create_sql=SHOW_CREATE_NO_SAMPLE)
    querylog_0008.update_querylog_table(pool, "default")
    assert pool.commands == []
    assert all(not q.startswith("SHOW CREATE") for q in pool.executed)


def test_update_querylog_table_rebuilds_when_sample_by_is_request_id() -> None:
    pool = _FakePool(sampling_key="request_id", create_sql=SHOW_CREATE_MULTILINE, row_count=0)
    querylog_0008.update_querylog_table(pool, "default")

    create_statements = [q for q in pool.executed if q.startswith("CREATE TABLE")]
    assert len(create_statements) == 1
    assert "SAMPLE BY" not in create_statements[0].upper()
    assert "querylog_local_new" in create_statements[0]
    assert "querylog_local," not in create_statements[0].split("\n")[0]
    assert pool.commands == [
        "RENAME TABLE querylog_local TO querylog_local_old;",
        "RENAME TABLE querylog_local_new TO querylog_local;",
        "DROP TABLE querylog_local_old;",
    ]


def test_update_querylog_table_copies_rows() -> None:
    pool = _FakePool(sampling_key="request_id", create_sql=SHOW_CREATE_INLINE, row_count=1)
    querylog_0008.update_querylog_table(pool, "default")

    inserts = [q for q in pool.executed if q.startswith("INSERT INTO")]
    assert len(inserts) == 1
    assert "querylog_local_new" in inserts[0]
    assert "FROM querylog_local" in inserts[0]


def test_update_querylog_table_ignores_other_sampling_keys() -> None:
    pool = _FakePool(sampling_key="cityHash64(request_id)", create_sql=SHOW_CREATE_NO_SAMPLE)
    querylog_0008.update_querylog_table(pool, "default")
    assert pool.commands == []
    assert all(not q.startswith("SHOW CREATE") for q in pool.executed)


def test_migration_is_registered() -> None:
    migrations = get_group_loader(MigrationGroup.QUERYLOG).get_migrations()
    assert "0008_drop_uuid_sample_by" in migrations
    loaded = get_group_loader(MigrationGroup.QUERYLOG).load_migration("0008_drop_uuid_sample_by")
    assert loaded.blocking is True


def test_forwards_visits_local_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    updated: list[str] = []

    def fake_update(clickhouse: Any, database: str) -> None:
        updated.append(database)

    monkeypatch.setattr(querylog_0008, "update_querylog_table", fake_update)

    mock_cluster = Mock()
    mock_cluster.get_local_nodes.return_value = [Mock(), Mock()]
    mock_cluster.get_database.return_value = "default"
    mock_cluster.get_node_connection.return_value = Mock()
    monkeypatch.setattr(querylog_0008, "get_cluster", lambda storage_set: mock_cluster)

    querylog_0008.forwards(Mock())
    assert updated == ["default", "default"]


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
