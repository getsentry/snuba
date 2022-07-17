import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Mapping, MutableSequence, Optional, Sequence

from snuba import environment, settings, util
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


def _get_metrics_tags(table: str, clickhouse_host: Optional[str]) -> Mapping[str, str]:
    return (
        {"table": table, "host": clickhouse_host}
        if clickhouse_host
        else {"table": table}
    )


def run_optimize(
    clickhouse: ClickhousePool,
    storage: ReadableTableStorage,
    database: str,
    before: Optional[datetime] = None,
    ignore_cutoff: bool = False,
    parallel: int = 1,
    clickhouse_host: Optional[str] = None,
) -> int:
    start = time.time()
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()

    parts = get_partitions_to_optimize(clickhouse, storage, database, table, before)
    optimize_partition_runner(
        clickhouse, database, table, parts, ignore_cutoff, parallel, clickhouse_host
    )
    metrics.timing(
        "optimized_all_parts",
        time.time() - start,
        tags=_get_metrics_tags(table, clickhouse_host),
    )
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


def _subdivide_parts(
    parts: Sequence[util.Part], number_of_subdivisions: int
) -> Sequence[Sequence[util.Part]]:
    """
    Subdivide a list of parts into number_of_subdivisions lists of parts so that optimize can be executed
    on different parts list by different threads.
    """

    sorted_parts = sorted(parts, key=lambda part: part.date, reverse=True)
    output: MutableSequence[Sequence[util.Part]] = []

    for i in range(number_of_subdivisions):
        output.append(sorted_parts[i::number_of_subdivisions])

    return output


def optimize_partition_runner(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[util.Part],
    ignore_cutoff: bool,
    parallel: int,
    clickhouse_host: Optional[str] = None,
) -> None:
    if parallel <= 1:
        optimize_partitions(
            clickhouse, database, table, parts, ignore_cutoff, clickhouse_host
        )
    else:
        divided_parts = _subdivide_parts(parts, parallel)
        threads: MutableSequence[threading.Thread] = []
        for i in range(0, parallel):
            threads.append(
                threading.Thread(
                    target=optimize_partitions,
                    args=(
                        clickhouse,
                        database,
                        table,
                        divided_parts[i],
                        ignore_cutoff,
                        clickhouse_host,
                    ),
                )
            )

            threads[i].start()

        for i in range(0, parallel):
            threads[i].join()


def optimize_partitions(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[util.Part],
    ignore_cutoff: bool,
    clickhouse_host: Optional[str] = None,
) -> None:
    query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION %(partition)s FINAL
    """

    # Adding 10 minutes to the current time before finding the midnight time
    # to ensure this keeps working even if the system clock of the host that
    # starts the pod is slightly ahead of the system clock of the host running
    # the job. This prevents us from getting the wrong midnight.
    last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if not ignore_cutoff:
        cutoff_time: Optional[datetime] = (
            last_midnight + settings.OPTIMIZE_JOB_CUTOFF_TIME
        )
        logger.info("Cutoff time: %s", str(cutoff_time))
    else:
        cutoff_time = None
        logger.info("Ignoring cutoff time")

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
        clickhouse.execute(query, retryable=False)
        metrics.timing(
            "optimized_part",
            time.time() - start,
            tags=_get_metrics_tags(table, clickhouse_host),
        )
