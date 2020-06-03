import logging

from clickhouse_driver import Client
from typing import MutableSequence

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas import Schema
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.migrations.parse_schema import get_local_schema


logger = logging.getLogger("snuba.migrate")

STORAGES_TO_MIGRATE = [
    StorageKey.EVENTS,
    StorageKey.ERRORS,
    StorageKey.GROUPEDMESSAGES,
    StorageKey.GROUPASSIGNEES,
    StorageKey.OUTCOMES_RAW,
    StorageKey.OUTCOMES_HOURLY,
    StorageKey.SESSIONS_RAW,
    StorageKey.SESSIONS_HOURLY,
    StorageKey.TRANSACTIONS,
]


def _run_schema(conn: Client, schema: Schema) -> None:
    if not isinstance(schema, TableSchema):
        return
    clickhouse_table = schema.get_local_table_name()

    local_schema = get_local_schema(conn, clickhouse_table)

    migrations = schema.get_migration_statements()(clickhouse_table, local_schema)
    for statement in migrations:
        logger.info(f"Executing migration: {statement}")
        conn.execute(statement)

    # Refresh after alters
    refreshed_schema = get_local_schema(conn, clickhouse_table)

    # Warn user about any *other* schema diffs
    differences = schema.get_column_differences(refreshed_schema)

    for difference in differences:
        logger.warn(difference)


def run() -> None:
    # Create the tables for all of the storages to be migrated.
    for storage_key in STORAGES_TO_MIGRATE:
        storage_name = storage_key.value
        logger.info("Creating tables for storage %s", storage_name)
        storage = get_storage(storage_key)
        conn = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

        for statement in storage.get_schemas().get_create_statements():
            logger.debug("Executing:\n%s", statement.statement)
            conn.execute(statement.statement)

        # Run migrations
        logger.info("Migrating storage %s", storage_name)
        schemas: MutableSequence[Schema] = []

        read_schema = storage.get_schemas().get_read_schema()
        write_schema = storage.get_schemas().get_write_schema()

        if write_schema:
            schemas.append(write_schema)

        schemas.append(read_schema)

        for schema in schemas:
            _run_schema(conn, schema)
