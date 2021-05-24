from snuba.clickhouse.http import JSONRowEncoder
from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.writer import BatchWriterEncoderWrapper
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


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
        `http_method` LowCardinality(Nullable(String)) DEFAULT CAST('', 'LowCardinality(Nullable(String))'),
        `http_referer` Nullable(String),
        `tags.key` Array(String), `tags.value` Array(String), `_tags_flattened` String,
        `contexts.key` Array(String), `contexts.value` Array(String), `_contexts_flattened` String,
        `partition` UInt16, `offset` UInt64, `message_timestamp` DateTime, `retention_days` UInt16,
        `deleted` UInt8) ENGINE = ReplacingMergeTree(deleted) PARTITION BY (retention_days, toMonday(finish_ts))
        ORDER BY (project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id))
        TTL finish_ts + toIntervalDay(retention_days);
        """
    )

    assert get_sampling_key() == ""
    generate_transactions()

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


def generate_transactions() -> None:
    from datetime import datetime

    table_writer = get_writable_storage(StorageKey.TRANSACTIONS).get_table_writer()

    rows = []

    for i in range(5):
        raw_transaction = get_raw_transaction()
        # Older versions of this table did not have measurements
        del raw_transaction["data"]["measurements"]
        del raw_transaction["data"]["breakdowns"]

        processed = (
            table_writer.get_stream_loader()
            .get_processor()
            .process_message(
                (2, "insert", raw_transaction),
                KafkaMessageMetadata(0, 0, datetime.utcnow()),
            )
        )
        rows.extend(processed.rows)

    BatchWriterEncoderWrapper(
        table_writer.get_batch_writer(metrics=DummyMetricsBackend(strict=True)),
        JSONRowEncoder(),
    ).write(rows)


def test_groupedmessages_compatibility() -> None:

    # these three commands are repeated
    cluster = get_cluster(StorageSetKey.EVENTS)
    database = cluster.get_database()
    connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

    # Create old style table witihout project ID
    connection.execute(
        """
        CREATE TABLE groupedmessage_local (`offset` UInt64, `record_deleted` UInt8,
        `id` UInt64, `status` Nullable(UInt8), `last_seen` Nullable(DateTime),
        `first_seen` Nullable(DateTime), `active_at` Nullable(DateTime),
        `first_release_id` Nullable(UInt64)) ENGINE = ReplacingMergeTree(offset)
        ORDER BY id SAMPLE BY id SETTINGS index_granularity = 8192
        """
    )

    migration_id = "0010_groupedmessages_onpremise_compatibility"

    # Runner() instantiation, run_migration(), update status
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    events_migrations = get_group_loader(MigrationGroup.EVENTS).get_migrations()

    # Mark prior migrations complete
    for migration in events_migrations[: (events_migrations.index(migration_id))]:
        runner._update_migration_status(
            MigrationKey(MigrationGroup.EVENTS, migration), Status.COMPLETED
        )

    runner.run_migration(
        MigrationKey(MigrationGroup.EVENTS, migration_id), force=True,
    )

    assert connection.execute(
        f"SELECT primary_key FROM system.tables WHERE name = 'groupedmessage_local' AND database = '{database}'"
    ) == [("project_id, id",)]


# provide a migration ID, group and run all the migrations that come before
def run_prior_migrations(migration_group, stop_migration_id, runner):

    right_migrations = next(
        group_migrations
        for (group, group_migrations) in runner.show_all()
        if group == migration_group
    )

    # Run migrations up to the desired 'stop' ID
    for migration in right_migrations:
        if migration.migration_id == stop_migration_id:
            break

        runner.run_migration(
            MigrationKey(migration_group, migration.migration_id), force=True
        )


# Below is an example of a valid test
# Write a similar test case potentially ?
def test_backfill_errors() -> None:

    backfill_migration_id = "0014_backfill_errors"
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))

    run_prior_migrations(MigrationGroup.EVENTS, backfill_migration_id, runner)

    # events_migrations = next(
    #     group_migrations
    #     for (group, group_migrations) in runner.show_all()
    #     if group == MigrationGroup.EVENTS
    # )

    # # Run migrations up 0014_backfill_errors
    # for migration in events_migrations:
    #     if migration.migration_id == backfill_migration_id:
    #         break

    #     runner.run_migration(
    #         MigrationKey(MigrationGroup.EVENTS, migration.migration_id), force=True
    #     )

    errors_storage = get_writable_storage(StorageKey.ERRORS)
    clickhouse = errors_storage.get_cluster().get_query_connection(
        ClickhouseClientSettings.QUERY
    )
    errors_table_name = errors_storage.get_table_writer().get_schema().get_table_name()

    def get_errors_count() -> int:
        return clickhouse.execute(f"SELECT count() from {errors_table_name}")[0][0]

    raw_events = []
    for i in range(10):
        event = get_raw_event()
        raw_events.append(event)

    events_storage = get_writable_storage(StorageKey.EVENTS)

    write_unprocessed_events(events_storage, raw_events)

    # write a function that will directly fill in data to the table

    assert get_errors_count() == 0

    # Run 0014_backfill_errors
    runner.run_migration(
        MigrationKey(MigrationGroup.EVENTS, backfill_migration_id), force=True
    )

    assert get_errors_count() == 10

    assert clickhouse.execute(
        f"SELECT contexts.key, contexts.value from {errors_table_name} LIMIT 1;"
    )[0] == (
        [
            "device.model_id",
            "geo.city",
            "geo.country_code",
            "geo.region",
            "os.kernel_version",
        ],
        ["Galaxy", "San Francisco", "US", "CA", "1.1.1"],
    )


# fetch some fields, still does some clickhouse query
