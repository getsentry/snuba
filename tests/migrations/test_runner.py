import importlib
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import factory
from snuba.datasets.storages.factory import STORAGES, get_storage
from snuba.migrations import MigrationGroup
from snuba.migrations.errors import MigrationError
from snuba.migrations.groups import ACTIVE_MIGRATION_GROUPS, get_group_loader
from snuba.migrations.parse_schema import get_local_schema
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


def _drop_all_tables() -> None:
    for cluster in CLUSTERS:
        connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
        database = cluster.get_database()

        data = connection.execute(
            f"SELECT name FROM system.tables WHERE database = '{database}'"
        )
        for (table,) in data:
            connection.execute(f"DROP TABLE IF EXISTS {table}")


def setup_function() -> None:
    _drop_all_tables()


def teardown_function() -> None:
    _drop_all_tables()


def test_get_status() -> None:
    runner = Runner()
    assert runner.get_status(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial")
    ) == (Status.NOT_STARTED, None)
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    assert runner.get_status(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial")
    ) == (Status.NOT_STARTED, None)
    runner.run_migration(MigrationKey(MigrationGroup.EVENTS, "0001_events_initial"))
    status = runner.get_status(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial")
    )
    assert status[0] == Status.COMPLETED
    assert isinstance(status[1], datetime)
    assert status[1] > datetime.now() - timedelta(seconds=1)


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

    # Running with --fake
    runner.run_migration(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial"), fake=True
    )
    assert connection.execute("SHOW TABLES LIKE 'sentry_local'") == []


def test_reverse_migration() -> None:
    runner = Runner()
    runner.run_all(force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )

    # Invalid migration ID
    with pytest.raises(MigrationError):
        runner.reverse_migration(MigrationKey(MigrationGroup.SYSTEM, "xxx"))

    with pytest.raises(MigrationError):
        runner.reverse_migration(MigrationKey(MigrationGroup.EVENTS, "0003_errors"))

    # Reverse with --fake
    for migration_id in reversed(
        get_group_loader(MigrationGroup.EVENTS).get_migrations()
    ):
        runner.reverse_migration(
            MigrationKey(MigrationGroup.EVENTS, migration_id), fake=True
        )
    assert (
        len(connection.execute("SHOW TABLES LIKE 'sentry_local'")) == 1
    ), "Table still exists"


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
    for group in ACTIVE_MIGRATION_GROUPS:
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
    settings.ENABLE_DEV_FEATURES = True
    importlib.reload(factory)
    runner = Runner()
    runner.run_all(force=True)

    for storage_key in STORAGES:
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

    importlib.reload(settings)
    importlib.reload(factory)


# def test_transactions_compatibility() -> None:
#     cluster = get_cluster(StorageSetKey.TRANSACTIONS)
#     connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

#     def get_sampling_key() -> str:
#         database = cluster.get_database()
#         ((sampling_key,),) = connection.execute(
#             f"SELECT sampling_key FROM system.tables WHERE name = 'transactions_local' AND database = '{database}'"
#         )
#         return sampling_key

#     # Create old style table without sampling expression and insert data
#     connection.execute(
#         """
#         CREATE TABLE transactions_local (`project_id` UInt64, `event_id` UUID,
#         `trace_id` UUID, `span_id` UInt64, `transaction_name` LowCardinality(String),
#         `transaction_hash` UInt64 MATERIALIZED CAST(cityHash64(transaction_name), 'UInt64'),
#         `transaction_op` LowCardinality(String), `transaction_status` UInt8 DEFAULT 2,
#         `start_ts` DateTime, `start_ms` UInt16, `finish_ts` DateTime, `finish_ms` UInt16,
#         `duration` UInt32, `platform` LowCardinality(String), `environment` LowCardinality(Nullable(String)),
#         `release` LowCardinality(Nullable(String)), `dist` LowCardinality(Nullable(String)),
#         `ip_address_v4` Nullable(IPv4), `ip_address_v6` Nullable(IPv6), `user` String DEFAULT '',
#         `user_hash` UInt64 MATERIALIZED cityHash64(user), `user_id` Nullable(String),
#         `user_name` Nullable(String), `user_email` Nullable(String),
#         `sdk_name` LowCardinality(String) DEFAULT CAST('', 'LowCardinality(String)'),
#         `sdk_version` LowCardinality(String) DEFAULT CAST('', 'LowCardinality(String)'),
#         `http_method` LowCardinality(Nullable(String)) DEFAULT CAST('', 'LowCardinality(Nullable(String))'),
#         `http_referer` Nullable(String),
#         `tags.key` Array(String), `tags.value` Array(String), `_tags_flattened` String,
#         `contexts.key` Array(String), `contexts.value` Array(String), `_contexts_flattened` String,
#         `partition` UInt16, `offset` UInt64, `message_timestamp` DateTime, `retention_days` UInt16,
#         `deleted` UInt8) ENGINE = ReplacingMergeTree(deleted) PARTITION BY (retention_days, toMonday(finish_ts))
#         ORDER BY (project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id))
#         TTL finish_ts + toIntervalDay(retention_days);
#         """
#     )

#     assert get_sampling_key() == ""
#     generate_transactions()

#     runner = Runner()
#     runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
#     runner._update_migration_status(
#         MigrationKey(MigrationGroup.TRANSACTIONS, "0001_transactions"), Status.COMPLETED
#     )
#     runner.run_migration(
#         MigrationKey(
#             MigrationGroup.TRANSACTIONS,
#             "0002_transactions_onpremise_fix_orderby_and_partitionby",
#         ),
#         force=True,
#     )

#     assert get_sampling_key() == "cityHash64(span_id)"

#     assert connection.execute("SELECT count(*) FROM transactions_local;") == [(5,)]


# def generate_transactions() -> None:
#     from datetime import datetime

#     table_writer = get_writable_storage(StorageKey.TRANSACTIONS).get_table_writer()

#     rows = []

#     for i in range(5):
#         raw_transaction = get_raw_transaction()
#         # Older versions of this table did not have measurements
#         del raw_transaction["data"]["measurements"]
#         del raw_transaction["data"]["breakdowns"]

#         processed = (
#             table_writer.get_stream_loader()
#             .get_processor()
#             .process_message(
#                 (2, "insert", raw_transaction),
#                 KafkaMessageMetadata(0, 0, datetime.utcnow()),
#             )
#         )
#         rows.extend(processed.rows)

#     BatchWriterEncoderWrapper(
#         table_writer.get_batch_writer(metrics=DummyMetricsBackend(strict=True)),
#         JSONRowEncoder(),
#     ).write(rows)


# def test_groupedmessages_compatibility() -> None:
#     cluster = get_cluster(StorageSetKey.EVENTS)
#     database = cluster.get_database()
#     connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

#     # Create old style table witihout project ID
#     connection.execute(
#         """
#         CREATE TABLE groupedmessage_local (`offset` UInt64, `record_deleted` UInt8,
#         `id` UInt64, `status` Nullable(UInt8), `last_seen` Nullable(DateTime),
#         `first_seen` Nullable(DateTime), `active_at` Nullable(DateTime),
#         `first_release_id` Nullable(UInt64)) ENGINE = ReplacingMergeTree(offset)
#         ORDER BY id SAMPLE BY id SETTINGS index_granularity = 8192
#         """
#     )

#     migration_id = "0010_groupedmessages_onpremise_compatibility"

#     runner = Runner()
#     runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
#     events_migrations = get_group_loader(MigrationGroup.EVENTS).get_migrations()

#     # Mark prior migrations complete
#     for migration in events_migrations[: (events_migrations.index(migration_id))]:
#         runner._update_migration_status(
#             MigrationKey(MigrationGroup.EVENTS, migration), Status.COMPLETED
#         )

#     runner.run_migration(
#         MigrationKey(MigrationGroup.EVENTS, migration_id), force=True,
#     )

#     assert connection.execute(
#         f"SELECT primary_key FROM system.tables WHERE name = 'groupedmessage_local' AND database = '{database}'"
#     ) == [("project_id, id",)]


# def test_backfill_errors() -> None:

#     backfill_migration_id = "0014_backfill_errors"
#     runner = Runner()
#     runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))

#     events_migrations = next(
#         group_migrations
#         for (group, group_migrations) in runner.show_all()
#         if group == MigrationGroup.EVENTS
#     )

#     # Run migrations up 0014_backfill_errors
#     for migration in events_migrations:
#         if migration.migration_id == backfill_migration_id:
#             break

#         runner.run_migration(
#             MigrationKey(MigrationGroup.EVENTS, migration.migration_id), force=True
#         )

#     errors_storage = get_writable_storage(StorageKey.ERRORS)
#     clickhouse = errors_storage.get_cluster().get_query_connection(
#         ClickhouseClientSettings.QUERY
#     )
#     errors_table_name = errors_storage.get_table_writer().get_schema().get_table_name()

#     def get_errors_count() -> int:
#         return clickhouse.execute(f"SELECT count() from {errors_table_name}")[0][0]

#     raw_events = []
#     for i in range(10):
#         event = get_raw_event()
#         raw_events.append(event)

#     events_storage = get_writable_storage(StorageKey.EVENTS)

#     write_unprocessed_events(events_storage, raw_events)

#     assert get_errors_count() == 0

#     # Run 0014_backfill_errors
#     runner.run_migration(
#         MigrationKey(MigrationGroup.EVENTS, backfill_migration_id), force=True
#     )

#     assert get_errors_count() == 10

#     assert clickhouse.execute(
#         f"SELECT contexts.key, contexts.value from {errors_table_name} LIMIT 1;"
#     )[0] == (
#         [
#             "device.model_id",
#             "geo.city",
#             "geo.country_code",
#             "geo.region",
#             "os.kernel_version",
#         ],
#         ["Galaxy", "San Francisco", "US", "CA", "1.1.1"],
#     )


def test_settings_skipped_group() -> None:
    from snuba.migrations import groups, runner

    with patch("snuba.settings.SKIPPED_MIGRATION_GROUPS", {"querylog", "metrics"}):
        importlib.reload(groups)
        importlib.reload(runner)
        runner.Runner().run_all(force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute("SHOW TABLES LIKE 'querylog_local'") == []
