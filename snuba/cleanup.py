from datetime import datetime, timedelta
import logging
import re


logger = logging.getLogger('snuba.cleanup')


# example partition name: "('2018-03-13 00:00:00', 90)"
PART_RE = re.compile(r"\('(\d{4}-\d{2}-\d{2}) 00:00:00', (\d+)\)")


def run_cleanup(clickhouse, database, table, dry_run=True):
    active_parts = get_active_partitions(clickhouse, database, table)
    stale_parts = filter_stale_partitions(active_parts)
    drop_partitions(clickhouse, database, table, stale_parts, dry_run=dry_run)
    return len(stale_parts)


def get_active_partitions(clickhouse, database, table):
    with clickhouse as ch:
        parts = ch.execute(
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

    ret = []
    for part in parts:
        match = PART_RE.match(part[0])
        if not match:
            raise ValueError("Unknown part name/format: " + str(part))

        date_str, retention_days = match.groups()
        date = datetime.strptime(date_str, '%Y-%m-%d')

        ret.append((date, int(retention_days)))

    return ret


def filter_stale_partitions(parts, as_of=None):
    if as_of is None:
        as_of = datetime.utcnow()

    return [
        (date, retention_days) for date, retention_days
        in parts
        if date < (as_of - timedelta(days=retention_days))
    ]


def drop_partitions(clickhouse, database, table, parts, dry_run=True):
    query_template = """\
        ALTER TABLE %(database)s.%(table)s DROP PARTITION ('%(date_str)s', %(retention_days)s)
    """

    with clickhouse as ch:
        for part_date, retention_days in parts:
            args = {
                'database': database,
                'table': table,
                'date_str': part_date.strftime("%Y-%m-%d %H:%M:%S"),
                'retention_days': retention_days,
            }

            query = (query_template % args).strip()
            if dry_run:
                logger.info("Dry run: " + query)
            else:
                logger.info("Dropping partition: " + query)
                ch.execute(query)
