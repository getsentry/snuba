from datetime import timedelta
from typing import Sequence
import logging

from snuba import util

logger = logging.getLogger("snuba.optimize")


def run_optimize(clickhouse, database, table, before=None):
    parts = get_partitions_to_optimize(clickhouse, database, table, before)
    optimize_partitions(clickhouse, database, table, parts)
    return len(parts)


def get_partitions_to_optimize(
    clickhouse, database, table, before=None
) -> Sequence[util.Part]:
    engine = clickhouse.execute(
        """
        SELECT engine
        FROM system.tables
        WHERE (database = %(database)s) AND (name = %(table)s)
        """,
        {"database": database, "table": table},
    )

    if not engine:
        logger.warning(
            "Table %s.%s doesn't exist on %s:%s"
            % (database, table, clickhouse.host, clickhouse.port)
        )
        return []

    if engine[0][0].startswith("Replicated"):
        is_leader = clickhouse.execute(
            """
            SELECT is_leader
            FROM system.replicas
            WHERE (database = %(database)s) AND (table = %(table)s)
            """,
            {"database": database, "table": table},
        )

        # response: [(0,)] for non-leader or [(1,)] for leader
        if not (len(is_leader) == 1 and is_leader[0][0]):
            return []

    active_parts = clickhouse.execute(
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
        {"database": database, "table": table},
    )

    parts = [util.decode_part_str(part) for part, count in active_parts]

    if before:
        parts = [
            p for p in parts if (p[0] + timedelta(days=6 - p[0].weekday())) < before
        ]

    return parts


def optimize_partitions(clickhouse, database, table, parts):
    query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION ('%(date_str)s', %(retention_days)s) FINAL
    """

    for part_date, retention_days in parts:
        date_str = part_date.strftime("%Y-%m-%d")
        args = {
            "database": database,
            "table": table,
            "date_str": date_str,
            "retention_days": retention_days,
        }

        query = (query_template % args).strip()
        logger.info("Optimizing partition: ('%s', %s)" % (date_str, retention_days))
        clickhouse.execute(query)
