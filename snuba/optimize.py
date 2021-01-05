from datetime import datetime, timedelta
from typing import Optional, Sequence
import logging

from snuba import util
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage

logger = logging.getLogger("snuba.optimize")


def run_optimize(
    clickhouse: ClickhousePool,
    storage_key: StorageKey,
    database: str,
    before: Optional[datetime] = None,
) -> int:
    storage = get_storage(storage_key)
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()

    parts = get_partitions_to_optimize(clickhouse, storage_key, database, table, before)
    optimize_partitions(clickhouse, storage_key, database, table, parts)
    return len(parts)


def get_partitions_to_optimize(
    clickhouse: ClickhousePool,
    storage_key: StorageKey,
    database: str,
    table: str,
    before: Optional[datetime] = None,
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

    parts = [util.decode_part_str(part, storage_key) for part, count in active_parts]

    if before:
        parts = [
            p for p in parts if (p[0] + timedelta(days=6 - p[0].weekday())) < before
        ]

    return parts


def optimize_partitions(
    clickhouse: ClickhousePool,
    storage_key: StorageKey,
    database: str,
    table: str,
    parts: Sequence[util.Part],
) -> None:

    events_query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION ('%(date_str)s', %(retention_days)s) FINAL
    """

    errors_transactions_query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION (%(retention_days)s, '%(date_str)s') FINAL
    """

    query_template = {
        StorageKey.EVENTS: events_query_template,
        StorageKey.ERRORS: errors_transactions_query_template,
        StorageKey.TRANSACTIONS: errors_transactions_query_template,
    }[storage_key]

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
