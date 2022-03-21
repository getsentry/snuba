import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Sequence

from snuba import environment, util
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.serializable_exception import SerializableException

logger = logging.getLogger("snuba.optimize")
metrics = MetricsWrapper(environment.metrics, "optimize")


class JobTimeoutException(SerializableException):
    """
    Signals that the job is past the cutoff time
    """

    pass


def run_optimize(
    clickhouse: ClickhousePool,
    storage: ReadableTableStorage,
    database: str,
    before: Optional[datetime] = None,
    cutoff_time: Optional[datetime] = None,
) -> int:
    start = time.time()
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()

    parts = get_partitions_to_optimize(clickhouse, storage, database, table, before)
    optimize_partitions(clickhouse, database, table, parts, cutoff_time)
    metrics.timing("optimized_all_parts", time.time() - start, tags={"table": table})
    return len(parts)


def get_partitions_to_optimize(
    clickhouse: ClickhousePool,
    storage: ReadableTableStorage,
    database: str,
    table: str,
    before: Optional[datetime] = None,
) -> Sequence[util.Part]:
    response = clickhouse.execute(
        """
        SELECT engine
        FROM system.tables
        WHERE (database = %(database)s) AND (name = %(table)s)
        """,
        {"database": database, "table": table},
    )

    if not response.results:
        logger.warning(
            "Table %s.%s doesn't exist on %s:%s"
            % (database, table, clickhouse.host, clickhouse.port)
        )
        return []

    if response.results[0][0].startswith("Replicated"):
        is_leader = clickhouse.execute(
            """
            SELECT is_leader
            FROM system.replicas
            WHERE (database = %(database)s) AND (table = %(table)s)
            """,
            {"database": database, "table": table},
        )
        if not is_leader:
            return []
        # response: [(0,)] for non-leader or [(1,)] for leader
        if not (len(is_leader.results) == 1 and is_leader.results[0][0]):
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
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    part_format = schema.get_part_format()
    assert part_format is not None

    parts = [
        util.decode_part_str(part, part_format) for part, count in active_parts.results
    ]

    if before:
        parts = [
            p for p in parts if (p.date + timedelta(days=6 - p.date.weekday())) < before
        ]

    return parts


def optimize_partitions(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[util.Part],
    cutoff_time: Optional[datetime] = None,
) -> None:

    query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION %(partition)s FINAL
    """

    for part in parts:
        if cutoff_time is not None and datetime.now() > cutoff_time:
            raise JobTimeoutException(
                "Optimize job is running past the cutoff time. Abandoning."
            )

        args = {
            "database": database,
            "table": table,
            "partition": part.name,
        }

        query = (query_template % args).strip()
        logger.info(f"Optimizing partition: {part.name}")
        start = time.time()
        clickhouse.execute(query)
        metrics.timing("optimized_part", time.time() - start, tags={"table": table})
