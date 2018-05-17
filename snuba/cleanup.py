from datetime import datetime, timedelta
import logging
import re


logger = logging.getLogger('snuba.cleanup')


# example partition name: "('2018-03-13 00:00:00', 90)"
PART_RE = re.compile(r"\('(\d{4}-\d{2}-\d{2}) 00:00:00', (\d+)\)")


def run_cleanup(clickhouse, database, table):
    active_parts = get_active_partitions(clickhouse, database, table)
    stale_parts = filter_stale_partitions(active_parts)
    return drop_partitions(clickhouse, stale_parts)


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
            raise RuntimeError("Unknown part name/format: " + str(part))

        date_str, retention_days = match.groups()
        date = datetime.strptime(date_str, '%Y-%m-%d')

        ret.append((date, retention_days))

    return ret


def filter_stale_partitions(parts):
    now = datetime.utcnow()

    ret = []
    for date, retention_days in parts:
        if date < (now - timedelta(dats=retention_days)):
            ret.append((date, retention_days))

    return ret


def drop_partitions(clickhouse, database, table, parts):
    ret = []
    with clickhouse as ch:
        resp = ch.execute(
            """
            ALTER TABLE %(database)s.%(table)s
            DROP PARTITION ('%(date_str)s', %(retention_days)s)
            """,
            {
                'database': database,
                'table': table,
                'date_str': '',  # TODO 0000-00-00 00:00:00
                'retention_days': 0,  # TODO
            }
        )
        ret.append(resp)

    return ret
