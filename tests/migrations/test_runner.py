import importlib
import pytest
from unittest.mock import patch

from snuba.clickhouse.http import JSONRowEncoder
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.migrations.errors import MigrationError
from snuba.migrations.groups import get_group_loader, MigrationGroup
from snuba.migrations.parse_schema import get_local_schema
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.writer import BatchWriterEncoderWrapper


def teardown_function() -> None:
    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    for table in [
        "migrations_local",
        "sentry_local",
        "transactions_local",
        "querylog_local",
    ]:
        connection.execute(f"DROP TABLE IF EXISTS {table};")


def test_show_all() -> None:
    runner = Runner()
    assert all(
        [
            migration.status == Status.NOT_STARTED
            for (_, group_migrations) in runner.show_all()
            for migration in group_migrations
        ]
    )
    runner.run_all(force=True)
    assert all(
        [
            migration.status == Status.COMPLETED
            for (_, group_migrations) in runner.show_all()
            for migration in group_migrations
        ]
    )


def test_run_migration() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute(
        "SELECT group, migration_id, status, version FROM migrations_local;"
    ) == [("system", "0001_migrations", "completed", 1)]

    # Invalid migration ID
    with pytest.raises(MigrationError):
        runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "xxx"))

    # Run out of order
    with pytest.raises(MigrationError):
        runner.run_migration(MigrationKey(MigrationGroup.EVENTS, "0003_errors"))


def test_reverse_migration() -> None:
    runner = Runner()
    runner.run_all(force=True)

    # Invalid migration ID
    with pytest.raises(MigrationError):
        runner.reverse_migration(MigrationKey(MigrationGroup.SYSTEM, "xxx"))

    with pytest.raises(MigrationError):
        runner.reverse_migration(MigrationKey(MigrationGroup.EVENTS, "0003_errors"))


def test_get_pending_migrations() -> None:
    runner = Runner()
    total_migrations = get_total_migration_count()
    assert len(runner._get_pending_migrations()) == total_migrations
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    assert len(runner._get_pending_migrations()) == total_migrations - 1


def test_run_all() -> None:
    runner = Runner()
    assert len(runner._get_pending_migrations()) == get_total_migration_count()

    with pytest.raises(MigrationError):
        runner.run_all(force=False)

    runner.run_all(force=True)
    assert runner._get_pending_migrations() == []


def test_reverse_all() -> None:
    runner = Runner()
    all_migrations = runner._get_pending_migrations()
    runner.run_all(force=True)
    for migration in reversed(all_migrations):
        runner.reverse_migration(migration, force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute("SHOW TABLES") == [], "All tables should be deleted"


def get_total_migration_count() -> int:
    count = 0
    for group in MigrationGroup:
        count += len(get_group_loader(group).get_migrations())
    return count


def test_version() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    migration_key = MigrationKey(MigrationGroup.EVENTS, "test")
    assert runner._get_next_version(migration_key) == 1
    runner._update_migration_status(migration_key, Status.IN_PROGRESS)
    assert runner._get_next_version(migration_key) == 2
    runner._update_migration_status(migration_key, Status.COMPLETED)
    assert runner._get_next_version(migration_key) == 3


def test_no_schema_differences() -> None:
    runner = Runner()
    runner.run_all(force=True)

    for storage_key in StorageKey:
        storage = get_storage(storage_key)
        conn = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

        schema = storage.get_schema()

        if not isinstance(schema, TableSchema):
            continue

        table_name = schema.get_local_table_name()
        local_schema = get_local_schema(conn, table_name)

        assert (
            schema.get_column_differences(local_schema) == []
        ), f"Schema mismatch: {table_name} does not match schema"


def test_transactions_compatibility() -> None:
    cluster = get_cluster(StorageSetKey.TRANSACTIONS)
    connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

    def get_sampling_key() -> str:
        database = cluster.get_database()
        ((sampling_key,),) = connection.execute(
            f"SELECT sampling_key FROM system.tables WHERE name = 'transactions_local' AND database = '{database}'"
        )
        return sampling_key

    # Create old style table without sampling expression and insert data
    connection.execute(
        """
        CREATE TABLE transactions_local (`project_id` UInt64, `event_id` UUID,
        `trace_id` UUID, `span_id` UInt64, `transaction_name` LowCardinality(String),
        `transaction_hash` UInt64 MATERIALIZED CAST(cityHash64(transaction_name), 'UInt64'),
        `transaction_op` LowCardinality(String), `transaction_status` UInt8 DEFAULT 2,
        `start_ts` DateTime, `start_ms` UInt16, `finish_ts` DateTime, `finish_ms` UInt16,
        `duration` UInt32, `platform` LowCardinality(String), `environment` LowCardinality(Nullable(String)),
        `release` LowCardinality(Nullable(String)), `dist` LowCardinality(Nullable(String)),
        `ip_address_v4` Nullable(IPv4), `ip_address_v6` Nullable(IPv6), `user` String DEFAULT '',
        `user_hash` UInt64 MATERIALIZED cityHash64(user), `user_id` Nullable(String),
        `user_name` Nullable(String), `user_email` Nullable(String),
        `sdk_name` LowCardinality(String) DEFAULT CAST('', 'LowCardinality(String)'),
        `sdk_version` LowCardinality(String) DEFAULT CAST('', 'LowCardinality(String)'),
        `tags.key` Array(String), `tags.value` Array(String), `_tags_flattened` String,
        `contexts.key` Array(String), `contexts.value` Array(String), `_contexts_flattened` String,
        `partition` UInt16, `offset` UInt64, `message_timestamp` DateTime, `retention_days` UInt16,
        `deleted` UInt8) ENGINE = ReplacingMergeTree(deleted) PARTITION BY (retention_days, toMonday(finish_ts))
        ORDER BY (project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id))
        TTL finish_ts + toIntervalDay(retention_days);
        """
    )

    assert get_sampling_key() == ""
    generate_transactions(5)

    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    runner._update_migration_status(
        MigrationKey(MigrationGroup.TRANSACTIONS, "0001_transactions"), Status.COMPLETED
    )
    runner.run_migration(
        MigrationKey(
            MigrationGroup.TRANSACTIONS,
            "0002_transactions_onpremise_fix_orderby_and_partitionby",
        ),
        force=True,
    )

    assert get_sampling_key() == "cityHash64(span_id)"

    assert connection.execute("SELECT count(*) FROM transactions_local;") == [(5,)]


def generate_transactions(count: int) -> None:
    """
    Generate a deterministic set of events across a time range.
    """
    import calendar
    import pytz
    import uuid
    from datetime import datetime, timedelta

    table_writer = get_writable_storage(StorageKey.TRANSACTIONS).get_table_writer()

    rows = []

    base_time = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=pytz.utc
    ) - timedelta(minutes=count)

    for tick in range(count):

        trace_id = "7400045b25c443b885914600aa83ad04"
        span_id = "8841662216cc598b"
        processed = (
            table_writer.get_stream_loader()
            .get_processor()
            .process_message(
                (
                    2,
                    "insert",
                    {
                        "project_id": 1,
                        "event_id": uuid.uuid4().hex,
                        "deleted": 0,
                        "datetime": (base_time + timedelta(minutes=tick)).isoformat(),
                        "platform": "javascript",
                        "data": {
                            # Project N sends every Nth (mod len(hashes)) hash (and platform)
                            "received": calendar.timegm(
                                (base_time + timedelta(minutes=tick)).timetuple()
                            ),
                            "type": "transaction",
                            "transaction": f"/api/do_things/{count}",
                            # XXX(dcramer): would be nice to document why these have to be naive
                            "start_timestamp": datetime.timestamp(
                                (base_time + timedelta(minutes=tick)).replace(
                                    tzinfo=None
                                )
                            ),
                            "timestamp": datetime.timestamp(
                                (
                                    base_time + timedelta(minutes=tick, seconds=1)
                                ).replace(tzinfo=None)
                            ),
                            "contexts": {
                                "trace": {
                                    "trace_id": trace_id,
                                    "span_id": span_id,
                                    "op": "http",
                                    "status": "0",
                                },
                            },
                            "spans": [
                                {
                                    "op": "db",
                                    "trace_id": trace_id,
                                    "span_id": span_id + "1",
                                    "parent_span_id": None,
                                    "same_process_as_parent": True,
                                    "description": "SELECT * FROM users",
                                    "data": {},
                                    "timestamp": calendar.timegm(
                                        (
                                            base_time + timedelta(minutes=tick)
                                        ).timetuple()
                                    ),
                                }
                            ],
                        },
                    },
                ),
                KafkaMessageMetadata(0, 0, base_time),
            )
        )
        rows.extend(processed.rows)

    BatchWriterEncoderWrapper(
        table_writer.get_writer(metrics=DummyMetricsBackend(strict=True)),
        JSONRowEncoder(),
    ).write(rows)


def test_settings_skipped_group() -> None:
    from snuba.migrations import groups, runner

    with patch("snuba.settings.SKIPPED_MIGRATION_GROUPS", {"querylog"}):
        importlib.reload(groups)
        importlib.reload(runner)
        runner.Runner().run_all(force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute("SHOW TABLES LIKE 'querylog_local'") == []
