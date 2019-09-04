"""\
Simple schema migration tool. Only intended for local development environment.
"""

import logging

logger = logging.getLogger('snuba.migrate')


def _run_schema(conn, schema):
    clickhouse_table = schema.get_local_table_name()
    get_schema = lambda: {
        column_name: column_type
        for column_name, column_type, default_type, default_expr
        in [cols[:4] for cols in conn.execute("DESCRIBE TABLE %s" % clickhouse_table)]
    }

    local_schema = get_schema()

    # Add/remove known migrations
    if 'group_id' not in local_schema:
        logger.info("Adding `group_id` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN group_id UInt64 DEFAULT 0" % clickhouse_table)

    if 'device_model' in local_schema:
        logger.info("Dropping unused `device_model` column.")
        conn.execute("ALTER TABLE %s DROP COLUMN device_model" % clickhouse_table)

    if 'sdk_integrations' not in local_schema:
        logger.info("Adding `sdk_integrations` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN sdk_integrations Array(String)" % clickhouse_table)

    if 'modules.name' not in local_schema:
        logger.info("Adding `modules` columns.")
        conn.execute("ALTER TABLE %s ADD COLUMN modules Nested(name String, version String)" % clickhouse_table)

    if 'culprit' not in local_schema:
        logger.info("Adding `culprit` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN culprit Nullable(String)" % clickhouse_table)

    if 'search_message' not in local_schema:
        logger.info("Adding `search_message` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN search_message Nullable(String)" % clickhouse_table)

    if 'title' not in local_schema:
        logger.info("Adding `title` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN title Nullable(String)" % clickhouse_table)

    if 'location' not in local_schema:
        logger.info("Adding `location` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN location Nullable(String)" % clickhouse_table)

    # Refresh after alters
    local_schema = get_schema()

    # Warn user about any *other* schema diffs
    differences = schema.get_schema_differences(local_schema)

    for difference in differences:
        logger.warn(difference)


def run(conn, dataset):
    schemas = [dataset.get_dataset_tables().get_read_schema(), dataset.get_dataset_tables().get_write_schema()]

    for schema in schemas:
        _run_schema(conn, schema)


def rename_dev_table(conn):
    # Migrate from the old events table to the new one if needed
    from snuba.datasets.factory import get_dataset
    local_table = get_dataset('events').get_dataset_tables().get_write_schema().get_local_table_name()
    clickhouse_tables = conn.execute('show tables')
    if not (local_table,) in clickhouse_tables and ("dev",) in clickhouse_tables:
        conn.execute("RENAME TABLE dev TO %s" % local_table)
