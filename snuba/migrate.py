"""
Simple schema migration tool. Only intended for local development environment.
"""

import logging

from clickhouse_driver import Client
from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.schemas.tables import MigrationSchemaColumn, TableSchema
from snuba.datasets.schemas import Schema

logger = logging.getLogger("snuba.migrate")


def _run_schema(conn: Client, schema: Schema) -> None:
    if not isinstance(schema, TableSchema):
        return
    clickhouse_table = schema.get_local_table_name()

    def get_schema():
        return {
            column_name: MigrationSchemaColumn(column_type, default_type, default_expr)
            for column_name, column_type, default_type, default_expr in [
                cols[:4]
                for cols in conn.execute("DESCRIBE TABLE %s" % clickhouse_table)
            ]
        }

    local_schema = get_schema()

    migrations = schema.get_migration_statements()(clickhouse_table, local_schema)
    for statement in migrations:
        logger.info(f"Executing migration: {statement}")
        conn.execute(statement)

    # Refresh after alters
    local_schema = get_schema()
    refreshed_schema = {col: col_desc[0] for col, col_desc in local_schema.items()}

    # Warn user about any *other* schema diffs
    differences = schema.get_column_differences(refreshed_schema)

    for difference in differences:
        logger.warn(difference)


def run(conn: Client, dataset: Dataset) -> None:
    schemas: Sequence[Schema] = []
    write_storage = dataset.get_writable_storage()
    if write_storage:
        writer = write_storage.get_table_writer()
        if writer:
            schemas.append(writer.get_schema())
    for storage in dataset.get_all_storages():
        schemas.append(storage.get_schemas().get_read_schema())

    for schema in schemas:
        _run_schema(conn, schema)
