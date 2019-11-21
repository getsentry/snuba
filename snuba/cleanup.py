from datetime import datetime, timedelta
import logging

from snuba import util


logger = logging.getLogger("snuba.cleanup")


def run_cleanup(clickhouse, database, table, dry_run=True):
    active_parts = get_active_partitions(clickhouse, database, table)
    stale_parts = filter_stale_partitions(active_parts)
    drop_partitions(clickhouse, database, table, stale_parts, dry_run=dry_run)
    return len(stale_parts)


def get_active_partitions(clickhouse, database, table):
    response = clickhouse.execute(
        """
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = %(database)s
        AND table = %(table)s
        AND active = 1
        """,
        {"database": database, "table": table},
    )

    return [util.decode_part_str(part) for part, in response]


def filter_stale_partitions(parts, as_of=None):
    """Filter partitions of (datetime, retention_days) down to ones
    that are out of the retention window based on `as_of` (default: now)."""

    if as_of is None:
        as_of = datetime.utcnow()

    stale_parts = []
    for part_date, retention_days in parts:
        part_last_day = part_date + timedelta(days=6 - part_date.weekday())

        if part_last_day < (as_of - timedelta(days=retention_days)):
            stale_parts.append((part_date, retention_days))

    return stale_parts


def drop_partitions(clickhouse, database, table, parts, dry_run=True):
    query_template = """\
        ALTER TABLE %(database)s.%(table)s DROP PARTITION ('%(date_str)s', %(retention_days)s)
    """

    for part_date, retention_days in parts:
        args = {
            "database": database,
            "table": table,
            "date_str": part_date.strftime("%Y-%m-%d"),
            "retention_days": retention_days,
        }

        query = (query_template % args).strip()
        if dry_run:
            logger.info("Dry run: " + query)
        else:
            logger.info("Dropping partition: " + query)
            clickhouse.execute(query)
