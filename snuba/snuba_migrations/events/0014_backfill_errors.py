import logging
from datetime import date, datetime, timedelta
from typing import Sequence

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import InsertIntoSelect


def format_date(day: date) -> str:
    return day.strftime("%Y-%m-%d")


def get_monday(day: date) -> date:
    return day - timedelta(days=day.weekday())


WINDOW = timedelta(days=7)
BEGINNING_OF_TIME = get_monday((datetime.utcnow() - timedelta(days=90)).date())


contexts_key = """
arrayMap(
    t -> t.1,
    arraySort(
        arrayFilter(
            t -> t.2 IS NOT NULL,
            arrayConcat(
                arrayZip(contexts.key, contexts.value),
                [('geo.city', geo_city)],
                [('geo.country_code', geo_country_code)],
                [('geo.region', geo_region)],
                [('os.build', os_build)],
                [('os.kernel_version', os_kernel_version)],
                [('device.name', device_name)],
                [('device.brand', device_brand)],
                [('device.locale', device_locale)],
                [('device.uuid', device_uuid)],
                [('device.model_id', device_model_id)],
                [('device.arch', device_arch)],
                [('device.battery_level', toString(device_battery_level))]
            )
        )
    ) AS tuplesArray
)
"""

contexts_value = """
    arrayMap(
        t -> t.2,
        tuplesArray
    )
"""


COLUMNS = [
    ("project_id", "project_id"),
    ("timestamp", "timestamp"),
    (
        "event_id",
        "toUUID(UUIDNumToString(toFixedString(unhex(toString(assumeNotNull(event_id))), 16)))",
    ),
    ("platform", "ifNull(`platform`, '')"),
    ("environment", "environment"),
    ("release", "`sentry:release`"),
    ("dist", "`sentry:dist`"),
    ("ip_address_v4", "if(position(ip_address, '.') > 0, toIPv4(`ip_address`), Null)"),
    ("ip_address_v6", "if(position(ip_address, ':') > 0, toIPv6(`ip_address`), Null)"),
    ("user", "ifNull(`sentry:user`, '')"),
    ("user_id", "user_id"),
    ("user_name", "username"),
    ("user_email", "email"),
    ("sdk_name", "sdk_name"),
    ("sdk_version", "sdk_version"),
    ("http_method", "http_method"),
    ("http_referer", "http_referer"),
    ("tags.key", "tags.key"),
    ("tags.value", "tags.value"),
    ("contexts.key", contexts_key),
    ("contexts.value", contexts_value),
    ("transaction_name", "ifNull(`transaction`, '')"),
    (
        "span_id",
        "reinterpretAsUInt64(reverse(unhex(upper(contexts.value[indexOf(contexts.key, 'trace.span_id')]))))",
    ),
    (
        "trace_id",
        "toUUID(UUIDNumToString(toFixedString(unhex(contexts.value[indexOf(contexts.key, 'trace.trace_id')]), 16)))",
    ),
    ("partition", "ifNull(`partition`, 0)"),
    ("offset", "ifNull(`offset`, 0)"),
    ("message_timestamp", "message_timestamp"),
    ("retention_days", "retention_days"),
    ("deleted", "deleted"),
    ("group_id", "group_id"),
    (
        "primary_hash",
        "toUUID(UUIDNumToString(toFixedString(unhex(toString(assumeNotNull(primary_hash))), 16)))",
    ),
    ("received", "ifNull(`received`, now())"),
    ("message", "ifNull(`message`, '')"),
    ("title", "ifNull(`title`, '')"),
    ("culprit", "ifNull(`culprit`, '')"),
    ("level", "level"),
    ("location", "location"),
    ("version", "version"),
    ("type", "ifNull(`type`, '')"),
    ("exception_stacks.type", "exception_stacks.type"),
    ("exception_stacks.value", "exception_stacks.value"),
    ("exception_stacks.mechanism_type", "exception_stacks.mechanism_type"),
    ("exception_stacks.mechanism_handled", "exception_stacks.mechanism_handled"),
    ("exception_frames.abs_path", "exception_frames.abs_path"),
    ("exception_frames.colno", "exception_frames.colno"),
    ("exception_frames.filename", "exception_frames.filename"),
    ("exception_frames.function", "exception_frames.function"),
    ("exception_frames.lineno", "exception_frames.lineno"),
    ("exception_frames.in_app", "exception_frames.in_app"),
    ("exception_frames.package", "exception_frames.package"),
    ("exception_frames.module", "exception_frames.module"),
    ("exception_frames.stack_level", "exception_frames.stack_level"),
    ("sdk_integrations", "sdk_integrations"),
    ("modules.name", "modules.name"),
    ("modules.version", "modules.version"),
]


def backfill_errors(logger: logging.Logger) -> None:
    from snuba.core.initialize import initialize_snuba
    from snuba.datasets.storages.factory import get_writable_storage
    from snuba.datasets.storages.storage_key import StorageKey

    initialize_snuba()  # ensure configuration is loaded
    errors_storage = get_writable_storage(StorageKey.ERRORS)
    cluster = errors_storage.get_cluster()
    database_name = cluster.get_database()

    events_local_table_name = "sentry_local"
    events_dist_table_name = "sentry_dist"
    events_table_name = (
        events_local_table_name
        if errors_storage.get_cluster().is_single_node()
        else events_dist_table_name
    )

    errors_table_name = errors_storage.get_table_writer().get_schema().get_table_name()

    if cluster.is_single_node():
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
    else:
        dist_node = cluster.get_distributed_nodes()[0]
        clickhouse = cluster.get_node_connection(
            ClickhouseClientSettings.MIGRATE, dist_node
        )

    try:
        (ts,) = clickhouse.execute(
            f"""
                SELECT timestamp FROM {errors_table_name}
                WHERE NOT deleted
                ORDER BY timestamp ASC, project_id ASC
                LIMIT 1
            """
        ).results[0]

        logger.info("Error data was found")
    except IndexError:
        ts = datetime.utcnow()

    timestamp = get_monday(ts.date())

    total_partitions = int((timestamp - BEGINNING_OF_TIME).days / 7)
    migrated_partitions = 0

    logger.info(f"Starting migration from {format_date(timestamp)}")

    while True:
        where = f"toMonday(timestamp) = toDate('{format_date(timestamp)}') AND (deleted = 0)"

        operation = InsertIntoSelect(
            storage_set=StorageSetKey.EVENTS,
            dest_table_name=errors_table_name,
            dest_columns=[c[0] for c in COLUMNS],
            src_table_name=events_table_name,
            src_columns=[c[1] for c in COLUMNS],
            prewhere="type != 'transaction'",
            where=where,
        )
        clickhouse.execute(operation.format_sql())

        migrated_partitions += 1

        logger.info(
            f"Migrated {format_date(timestamp)}. ({migrated_partitions} of {total_partitions} partitions done)"
        )

        timestamp -= WINDOW

        if timestamp <= BEGINNING_OF_TIME:
            logger.info("Done. Optimizing.")
            break

    if cluster.is_single_node():
        partitions = clickhouse.execute(
            """
            SELECT partition, count() as c
            FROM system.parts
            WHERE active
            AND database = %(database)s
            AND table = %(table)s
            GROUP BY partition
            HAVING c > 1
            ORDER BY c DESC, partition
            """,
            {"database": database_name, "table": errors_table_name},
        ).results

        for partition, _count in partitions:
            clickhouse.execute(
                f"""
                OPTIMIZE TABLE {database_name}.{errors_table_name}
                PARTITION {partition} FINAL DEDUPLICATE
                """
            )


class Migration(migration.CodeMigration):
    """
    Backfills the errors table from events.
    Errors replacements should be turned off while this script is running.
    Note this migration is not reversible.
    """

    blocking = True

    def forwards_global(self) -> Sequence[operations.RunPython]:

        return [
            operations.RunPython(
                func=backfill_errors, description="Backfill errors table from events"
            ),
        ]

    def backwards_global(self) -> Sequence[operations.RunPython]:
        return []
