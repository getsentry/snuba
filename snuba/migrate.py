"""\
Simple schema migration tool. Only intended for local development environment.
"""

import logging

logger = logging.getLogger('snuba.cleanup')


def run(conn, clickhouse_table):
    from snuba.clickhouse import ALL_COLUMNS

    get_schema = lambda: {
        column_name: column_type
        for column_name, column_type, default_type, default_expr
        in conn.execute("DESCRIBE TABLE %s" % clickhouse_table)
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

    # Refresh after alters
    local_schema = get_schema()

    # Warn user about any *other* schema diffs
    for column_name, column_type in local_schema.items():
        if column_name not in ALL_COLUMNS:
            logger.warn("Column '%s' exists in local ClickHouse but not in schema!", column_name)
            continue

        expected_type = ALL_COLUMNS[column_name].type.for_schema()
        if column_type != expected_type:
            logger.warn(
                "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)",
                column_name,
                expected_type,
                column_type
            )
