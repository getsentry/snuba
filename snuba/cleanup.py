from datetime import datetime, timedelta
import logging

from snuba import util


logger = logging.getLogger('snuba.cleanup')




def run_cleanup(clickhouse, database, table, dry_run=True):
    active_parts = get_active_partitions(clickhouse, database, table)
    stale_parts = filter_stale_partitions(active_parts)
    drop_partitions(clickhouse, database, table, stale_parts, dry_run=dry_run)
    return len(stale_parts)


def get_active_partitions(clickhouse, database, table):
    with clickhouse as ch:
        response = ch.execute(
            """
            SELECT partition
            FROM system.parts
            WHERE database = %(database)s
            AND table = %(table)s
            AND active = 1
            """,
            {
                'database': database,
                'table': table,
            }
        )

    return [util.decode_part_str(part) for part, in response]


def filter_stale_partitions(parts, as_of=None):
    """Filter partitions of (datetime, retention_days) down to ones
    that are out of the retention window based on `as_of` (default: now)."""

    if as_of is None:
        as_of = datetime.utcnow()

    return [part for part in parts if part[0] < (as_of - timedelta(days=part[1]))]


def drop_partitions(clickhouse, database, table, parts, dry_run=True):
    query_template = """\
        ALTER TABLE %(database)s.%(table)s DROP PARTITION ('%(date_str)s', %(retention_days)s)
    """

    with clickhouse as ch:
        for part_date, retention_days in parts:
            args = {
                'database': database,
                'table': table,
                'date_str': part_date.strftime("%Y-%m-%d"),
                'retention_days': retention_days,
            }

            query = (query_template % args).strip()
            if dry_run:
                logger.info("Dry run: " + query)
            else:
                logger.info("Dropping partition: " + query)
                ch.execute(query)
