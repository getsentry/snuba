"""\
Simple schema migration tool. Only intended for local development environment.
"""

import logging

logger = logging.getLogger('snuba.migrate')


def run(conn, SCHEMA):

    clickhouse_table = SCHEMA.QUERY_TABLE

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
    for column_name, column_type in local_schema.items():
        if column_name not in SCHEMA.ALL_COLUMNS:
            logger.warn("Column '%s' exists in local ClickHouse but not in schema!", column_name)
            continue

        expected_type = SCHEMA.ALL_COLUMNS[column_name].type.for_schema()
        if column_type != expected_type:
            logger.warn(
                "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)",
                column_name,
                expected_type,
                column_type
            )
