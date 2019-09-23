"""\
Simple schema migration tool. Only intended for local development environment.
"""

import logging

from snuba.datasets.factory import enforce_table_writer
from snuba.datasets.schemas.tables import TableSchema

logger = logging.getLogger('snuba.migrate')


def _run_schema(conn, schema):
    if not isinstance(schema, TableSchema):
        return
    clickhouse_table = schema.get_local_table_name()
    get_schema = lambda: {
        column_name: column_type
        for column_name, column_type, default_type, default_expr
        in [cols[:4] for cols in conn.execute("DESCRIBE TABLE %s" % clickhouse_table)]
    }

    local_schema = get_schema()

    migrations = schema.get_migration_statements()(clickhouse_table, local_schema)
    for statement in migrations:
        logger.info(f"Executing migration: {statement}")
        conn.execute(statement)

    # Refresh after alters
    local_schema = get_schema()

    # Warn user about any *other* schema diffs
    differences = schema.get_column_differences(local_schema)

    for difference in differences:
        logger.warn(difference)


def run(conn, dataset):
    schemas = [
        dataset.get_dataset_schemas().get_read_schema(),
        enforce_table_writer(dataset).get_schema(),
    ]

    for schema in schemas:
        _run_schema(conn, schema)
