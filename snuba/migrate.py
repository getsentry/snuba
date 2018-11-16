"""\
Simple schema migration tool. Only intended for local development environment.
"""

import logging

logger = logging.getLogger('snuba.cleanup')


def run(conn, clickhouse_table):
    describe = conn.execute("DESCRIBE TABLE %s" % clickhouse_table)

    schema = {}
    for column, type_, default_type, default_expression in describe:
        schema[column] = type_

    if 'group_id' not in schema:
        logger.info("Adding `group_id` column.")
        conn.execute("ALTER TABLE %s ADD COLUMN group_id DEFAULT 0" % clickhouse_table)
