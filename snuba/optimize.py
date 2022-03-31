import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Mapping, MutableSequence, Optional, Sequence

from snuba import environment, settings, util
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.state import rds
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.serializable_exception import SerializableException

logger = logging.getLogger("snuba.optimize")
metrics = MetricsWrapper(environment.metrics, "optimize")


class JobTimeoutException(SerializableException):
    """
    Signals that the job is past the cutoff time
    """

    pass


class OptimizedPartitionTracker:
    def __init__(self, host: str):
        self.__host = host
        self.__bucket = f"optimize_prefix.{self.__host}"
        self.__key_expire_time = (
            datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            + settings.OPTIMIZE_JOB_CUTOFF_TIME
        )

    def get_completed_partitions(self) -> Optional[Sequence[str]]:
        completed_partitions = rds.smembers(self.__bucket)
        if not completed_partitions:
            return None

        partitions_list: MutableSequence[str] = []
        for partition in completed_partitions:
            assert isinstance(partition, bytes)
            partitions_list.append(partition.decode("utf-8"))

        return partitions_list

    def update_completed_partitions(self, part_name: str) -> None:
        rds.sadd(self.__bucket, part_name.encode("utf-8"))
        rds.expireat(self.__bucket, self.__key_expire_time)

    def remove_all_partitions(self) -> None:
        rds.delete(self.__bucket)


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
    parallel: int = 1,
    cutoff_time: Optional[datetime] = None,
    clickhouse_host: Optional[str] = None,
) -> int:
    start = time.time()
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()
    partition_tracker = (
        OptimizedPartitionTracker(clickhouse_host) if clickhouse_host else None
    )
    parts = get_partitions_to_optimize(clickhouse, storage, database, table, before)

    if partition_tracker:
        completed_partitions = partition_tracker.get_completed_partitions()
        parts = remove_partitions_already_completed(parts, completed_partitions)

    optimize_partition_runner(
        clickhouse, database, table, parts, parallel, cutoff_time, None, clickhouse_host
    )
    if partition_tracker:
        partition_tracker.remove_all_partitions()
    metrics.timing(
        "optimized_all_parts",
        time.time() - start,
        tags=_get_metrics_tags(table, clickhouse_host),
    )
    return len(parts)


def remove_partitions_already_completed(
    parts: Sequence[util.Part], completed_partitions: Optional[Sequence[str]]
) -> Sequence[util.Part]:
    return (
        [part for part in parts if part.name in set(completed_partitions)]
        if completed_partitions
        else parts
    )


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
    parallel: int,
    cutoff_time: Optional[datetime],
    optimize_partition_tracker: Optional[OptimizedPartitionTracker],
    clickhouse_host: Optional[str] = None,
) -> None:
    """
    The runner provides a mechanism for running the optimize_partitions call.

    For non-parallel mode, it simply calls the optimize_partitions method.

    For parallel mode, it subdivides the parts into number of threads which are needed to be run.
    It also provides a different cutoff_time to avoid multiple executions of optimize_partitions
    during peak traffic time.
    """
    if parallel <= 1:
        optimize_partitions(
            clickhouse=clickhouse,
            database=database,
            table=table,
            parts=parts,
            cutoff_time=cutoff_time,
            optimize_partition_tracker=optimize_partition_tracker,
            clickhouse_host=clickhouse_host,
        )
    else:
        # Compute threaded cutoff_time
        last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        threaded_cutoff_time = (
            last_midnight + settings.PARALLEL_OPTIMIZE_JOB_CUTOFF_TIME
        )

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
                        threaded_cutoff_time,
                        optimize_partition_tracker,
                        clickhouse_host,
                    ),
                )
            )

            threads[i].start()

        # Wait for all threads to finish. Any thread can raise JobTimeoutException. Its an indication that not all
        # parts could be optimized.
        for i in range(0, parallel):
            try:
                threads[i].join()
            except JobTimeoutException:
                pass

        if not optimize_partition_tracker:
            return

        # Find out parts which haven't been optimized.
        remaining_parts = remove_partitions_already_completed(
            parts, optimize_partition_tracker.get_completed_partitions()
        )
        # Call optimize_partitions with the original cutoff_time
        optimize_partitions(
            clickhouse=clickhouse,
            database=database,
            table=table,
            parts=remaining_parts,
            cutoff_time=cutoff_time,
            optimize_partition_tracker=optimize_partition_tracker,
            clickhouse_host=clickhouse_host,
        )


def optimize_partitions(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[util.Part],
    cutoff_time: Optional[datetime],
    optimize_partition_tracker: Optional[OptimizedPartitionTracker],
    clickhouse_host: Optional[str] = None,
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
        if optimize_partition_tracker:
            optimize_partition_tracker.update_completed_partitions(part.name)
        metrics.timing(
            "optimized_part",
            time.time() - start,
            tags=_get_metrics_tags(table, clickhouse_host),
        )
