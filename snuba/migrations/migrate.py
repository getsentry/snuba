import logging

from clickhouse_driver import Client

from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas import Schema
from snuba.datasets.schemas.tables import TableSchema, TableSchemaWithDDL
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
    # Ensure all clusters are single node before attempting to migrate
    assert all(
        cluster.is_single_node() for cluster in CLUSTERS
    ), "Cannot run migrations for multi node clusters"

    # Create the tables for all of the storages to be migrated.
    for storage_key in STORAGES_TO_MIGRATE:
        run_storage(storage_key)

    # Additional schemas to be migrated
    from snuba.datasets.storages.outcomes import (
        materialized_view_schema as outcomes_mv_schema,
    )
    from snuba.datasets.storages.sessions import (
        materialized_view_schema as sessions_mv_schema,
    )

    ADDITIONAL_SCHEMAS_TO_MIGRATE = [
        (StorageSetKey.OUTCOMES, outcomes_mv_schema),
        (StorageSetKey.SESSIONS, sessions_mv_schema),
    ]

    for storage_set, schema in ADDITIONAL_SCHEMAS_TO_MIGRATE:
        conn = get_cluster(storage_set).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        conn.execute(schema.get_local_table_definition().statement)


def run_storage(storage_key: StorageKey) -> None:
    storage_name = storage_key.value
    logger.info("Creating tables for storage %s", storage_name)
    storage = get_storage(storage_key)
    conn = storage.get_cluster().get_query_connection(ClickhouseClientSettings.MIGRATE)

    schema = storage.get_schema()

    if isinstance(schema, TableSchemaWithDDL):
        statement = schema.get_local_table_definition()
        logger.debug("Executing:\n%s", statement.statement)
        conn.execute(statement.statement)

    # Run migrations
    logger.info("Migrating storage %s", storage_name)

    _run_schema(conn, storage.get_schema())
