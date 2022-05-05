import logging
import re
import threading
import time
from dataclasses import dataclass
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


@dataclass(frozen=True)
class OptimizationBucket:
    parallel: int
    cutoff_time: datetime


def _build_optimization_buckets(
    start_time: datetime, max_parallel: int
) -> Sequence[OptimizationBucket]:
    """
    Build buckets of optimization jobs.

    Each bucket represents the number of threads that can run the
    optimization and the time till the job can be run with that many threads.
    """
    last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    if max_parallel == 1:
        return [
            OptimizationBucket(
                parallel=1,
                cutoff_time=last_midnight + settings.OPTIMIZE_JOB_CUTOFF_TIME,
            )
        ]
    else:
        parallel_start_time = last_midnight + settings.PARALLEL_OPTIMIZE_JOB_START_TIME
        parallel_end_time = last_midnight + settings.PARALLEL_OPTIMIZE_JOB_END_TIME
        full_job_end_time = last_midnight + settings.OPTIMIZE_JOB_CUTOFF_TIME
        pre_parallel_bucket = OptimizationBucket(
            parallel=1,
            cutoff_time=parallel_start_time,
        )
        parallel_bucket = OptimizationBucket(
            parallel=max_parallel,
            cutoff_time=parallel_end_time,
        )
        final_bucket = OptimizationBucket(
            parallel=1,
            cutoff_time=full_job_end_time,
        )

        if start_time < parallel_start_time:
            return [pre_parallel_bucket, parallel_bucket, final_bucket]
        elif start_time < parallel_end_time:
            return [parallel_bucket, final_bucket]
        else:
            return [final_bucket]


def run_optimize(
    clickhouse: ClickhousePool,
    storage: ReadableTableStorage,
    database: str,
    before: Optional[datetime] = None,
) -> int:
    """
    The most basic form of running an optimize final on a storage.

    It is deliberately simple and separate from `run_optimize_cron_job` since
    there is a need to be able to run optimizations from the replacer.
    """
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()

    parts = get_partitions_to_optimize(clickhouse, storage, database, table, before)
    part_names = [part.name for part in parts]

    optimize_partitions(
        clickhouse=clickhouse,
        database=database,
        table=table,
        parts=part_names,
    )

    return len(parts)


def run_optimize_cron_job(
    clickhouse: ClickhousePool,
    storage: ReadableTableStorage,
    database: str,
    parallel: int,
    clickhouse_host: str,
    tracker: OptimizedPartitionTracker,
    before: Optional[datetime] = None,
) -> int:
    """
    The sophisticated form of running an optimize final on a storage.

    The sophistication include:
    1. Being able to run multiple optimize final jobs concurrently.
    2. Being able to set time boundaries for different phases of optimization.
    3. Being able to work with stateful optimization states.
    """
    start = time.time()
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    database = storage.get_cluster().get_database()

    optimization_bucket = _build_optimization_buckets(
        start_time=datetime.now(), max_parallel=parallel
    )

    if not tracker.get_all_parts():
        parts = get_partitions_to_optimize(clickhouse, storage, database, table, before)
        if len(parts) == 0:
            logger.info("No partitions need optimization")
            return 0
        part_names = [part.name for part in parts]
        logger.info(f"All partitions list: {part_names}")
        tracker.update_all_parts(part_names)

    parts_to_optimize = tracker.get_parts_to_optimize()
    if parts_to_optimize is None or len(parts_to_optimize) == 0:
        logger.info("No partitions need optimization")
        return 0

    optimize_partition_runner(
        clickhouse=clickhouse,
        database=database,
        table=table,
        parts=list(parts_to_optimize),
        optimization_buckets=optimization_bucket,
        tracker=tracker,
        clickhouse_host=clickhouse_host,
    )

    metrics.timing(
        "optimized_all_parts",
        time.time() - start,
        tags=_get_metrics_tags(table, clickhouse_host),
    )
    return len(parts_to_optimize)


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
    parts: Sequence[str], number_of_subdivisions: int
) -> Sequence[Sequence[str]]:
    """
    Subdivide a list of parts into number_of_subdivisions lists of parts
    so that optimize can be executed on different parts list by different
    threads.
    """

    def sort_partitions(partition_name: str) -> str:
        date_regex = re.compile("\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])")

        match = re.search(date_regex, partition_name)
        if match is not None:
            return match.group()

        return partition_name

    if number_of_subdivisions == 1:
        return [parts]

    sorted_parts = sorted(parts, key=sort_partitions, reverse=True)
    output: MutableSequence[Sequence[str]] = []

    for i in range(number_of_subdivisions):
        output.append(sorted_parts[i::number_of_subdivisions])

    return output


def optimize_partition_runner(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[str],
    optimization_buckets: Sequence[OptimizationBucket],
    tracker: OptimizedPartitionTracker,
    clickhouse_host: str,
) -> None:
    """
    Run optimize jobs in threads based on the optimization buckets.

    It subdivides the partitions into number of threads which are needed
    to be run. Each thread then runs the optimize job on the list of
    partitions assigned to it.

    The threads run the optimization job based on the optimization bucket.

    We traverse through all the optimization buckets until
    1. All optimizations are done.
    2. The final optimization bucket id done.
    """
    for bucket in optimization_buckets:
        parallel, cutoff_time = bucket.parallel, bucket.cutoff_time
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
                        cutoff_time,
                        tracker,
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

        # If there are still partitions needing optimization then move on to the
        # next bucket with the parts which still need optimization.

        remaining_parts = tracker.get_parts_to_optimize()
        if remaining_parts:
            parts = list(remaining_parts)
        else:
            return


def optimize_partitions(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[str],
    cutoff_time: Optional[datetime] = None,
    tracker: Optional[OptimizedPartitionTracker] = None,
    clickhouse_host: Optional[str] = None,
) -> None:
    query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION %(partition)s FINAL
    """

    for part_name in parts:
        if cutoff_time is not None and datetime.now() > cutoff_time:
            raise JobTimeoutException(
                "Optimize job is running past the cutoff time. Abandoning."
            )

        args = {
            "database": database,
            "table": table,
            "partition": part_name,
        }

        query = (query_template % args).strip()
        logger.info(f"Optimizing partition: {part_name}")
        start = time.time()

        # Update tracker before running clickhouse.execute since its possible
        # that the connection can get disconnected while the server is still
        # executing the optimization. In that case we don't want to
        # run optimization on the same partition twice.
        if tracker:
            tracker.update_completed_parts(part_name)

        clickhouse.execute(query)
        metrics.timing(
            "optimized_part",
            time.time() - start,
            tags=_get_metrics_tags(table, clickhouse_host),
        )
