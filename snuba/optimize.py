from datetime import timedelta
import logging

from snuba import util


logger = logging.getLogger('snuba.optimize')


def run_optimize(clickhouse, database, table, before=None):
    parts = get_partitions_to_optimize(clickhouse, database, table, before)
    optimize_partitions(clickhouse, database, table, parts)
    return len(parts)


def get_partitions_to_optimize(clickhouse, database, table, before=None):
    response = clickhouse.execute(
        """
        SELECT
            partition,
            count() AS c
        FROM system.parts
        WHERE active
        AND database = %(database)s
        AND table = %(table)s
        GROUP BY partition
        HAVING c > 1
        ORDER BY c DESC, partition
        """,
        {
            'database': database,
            'table': table,
        }
    )

    parts = [util.decode_part_str(part) for part, count in response]

    if before:
        parts = filter(lambda p: (p[0] + timedelta(days=6 - p[0].weekday())) < before, parts)

    return parts


def optimize_partitions(clickhouse, database, table, parts):
    query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION ('%(date_str)s', %(retention_days)s) FINAL
    """

    for part_date, retention_days in parts:
        date_str = part_date.strftime("%Y-%m-%d")
        args = {
            'database': database,
            'table': table,
            'date_str': date_str,
            'retention_days': retention_days,
        }

        query = (query_template % args).strip()
        logger.info("Optimizing partition: ('%s', %s)" % (date_str, retention_days))
        clickhouse.execute(query)
