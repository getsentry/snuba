import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Mapping, MutableSequence, Optional, Sequence

from snuba import environment, settings, util
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.optimize_tracker import OptimizedPartitionTracker
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
    parallel: int = 1,
    cutoff_time: Optional[datetime] = None,
    clickhouse_host: Optional[str] = None,
    optimize_partition_tracker: Optional[OptimizedPartitionTracker] = None,
) -> int:
    start = time.time()
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()
    parts = get_partitions_to_optimize(clickhouse, storage, database, table, before)

    if optimize_partition_tracker:
        completed_partitions = optimize_partition_tracker.get_completed_partitions()
        parts = (
            [part for part in parts if part.name not in completed_partitions]
            if completed_partitions
            else parts
        )

    if not len(parts):
        logger.info("No partitions to optimize")
        return 0

    optimize_partition_runner(
        clickhouse=clickhouse,
        database=database,
        table=table,
        parts=parts,
        parallel=parallel,
        cutoff_time=cutoff_time,
        optimize_partition_tracker=optimize_partition_tracker,
        clickhouse_host=clickhouse_host,
    )

    if optimize_partition_tracker:
        optimize_partition_tracker.remove_all_partitions()

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
    Subdivide a list of parts into number_of_subdivisions lists of parts
    so that optimize can be executed on different parts list by different
    threads.
    """

    sorted_parts = sorted(parts, key=lambda part: part.date, reverse=True)
    output: MutableSequence[Sequence[util.Part]] = []

    for i in range(number_of_subdivisions):
        output.append(sorted_parts[i::number_of_subdivisions])

    return output


def concurrent_optimize_partition_runner(
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
    Run optimize job concurrently using multiple threads.

    It subdivides the partitions into number of threads which are needed
    to be run. Each thread then runs the optimize job on the list of
    partitions assigned to it.

    The threads are provides a different cutoff_time than what is
    provided when calling this function. This is to avoid multiple
    executions of optimize_partitions during peak traffic time.

    If the threads are unable to process all partitions before
    cutoff_time is reached, the optimize job is run serially until the
    original cutoff_time provided in the function call.
    """
    threaded_cutoff_time: Optional[datetime] = None
    if cutoff_time:
        # Compute threaded cutoff_time
        last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        threaded_cutoff_time = (
            last_midnight + settings.PARALLEL_OPTIMIZE_JOB_CUTOFF_TIME
        )
        logger.info(f"Threaded cutoff time: {threaded_cutoff_time}")

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

    # Wait for all threads to finish. Any thread can raise
    # JobTimeoutException. Its an indication that not all partitions
    # could be optimized in the given amount of threaded_cutoff_time.
    for i in range(0, parallel):
        try:
            threads[i].join()
        except JobTimeoutException:
            pass

    if not optimize_partition_tracker:
        return

    # Find out partitions which couldn't be optimized because of threaded
    # cutoff time.
    partitions = optimize_partition_tracker.get_completed_partitions()
    remaining_parts = (
        [part for part in parts if part.name not in partitions] if partitions else parts
    )

    if len(remaining_parts):
        # Call optimize_partitions with the original cutoff_time now since
        # running optimizations serially is allowed for a larger duration.
        optimize_partitions(
            clickhouse=clickhouse,
            database=database,
            table=table,
            parts=remaining_parts,
            cutoff_time=cutoff_time,
            optimize_partition_tracker=optimize_partition_tracker,
            clickhouse_host=clickhouse_host,
        )


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
    Optimize partition runner decides whether to run the threaded
    version of optimization or the non-threaded version.
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
        return

    # Phase 1: Cannot run parallel optimization until PARALLEL_JOB_START_TIME
    # is reached.
    try:
        non_parallel_cutoff_time: Optional[datetime] = None
        if cutoff_time:
            # Compute non parallel cutoff_time
            last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            non_parallel_cutoff_time = last_midnight + settings.PARALLEL_JOB_START_TIME
            logger.info(f"Non parallel cutoff time: {non_parallel_cutoff_time}")

        optimize_partitions(
            clickhouse=clickhouse,
            database=database,
            table=table,
            parts=parts,
            cutoff_time=non_parallel_cutoff_time,
            optimize_partition_tracker=optimize_partition_tracker,
            clickhouse_host=clickhouse_host,
        )
    except JobTimeoutException:
        # TODO
        pass

    concurrent_optimize_partition_runner(
        clickhouse=clickhouse,
        database=database,
        table=table,
        parts=parts,
        parallel=parallel,
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

        # Update tracker before running clickhouse.execute since its possible
        # that the connection can get disconnected while the server is still
        # executing the optimization. In that case we don't want to
        # run optimization on the same partition twice.
        if optimize_partition_tracker:
            optimize_partition_tracker.update_completed_partitions(part.name)
        clickhouse.execute(query)
        metrics.timing(
            "optimized_part",
            time.time() - start,
            tags=_get_metrics_tags(table, clickhouse_host),
        )
