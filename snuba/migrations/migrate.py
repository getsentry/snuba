import logging

from snuba.datasets.schemas.tables import TableSchema
from snuba.migrations.parse_schema import get_local_schema

logger = logging.getLogger("snuba.migrate")


def _run_schema(conn, schema):
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


def run(conn, dataset):
    schemas = []
    if dataset.get_table_writer():
        schemas.append(dataset.get_table_writer().get_schema())
    schemas.append(dataset.get_dataset_schemas().get_read_schema())

    for schema in schemas:
        _run_schema(conn, schema)
