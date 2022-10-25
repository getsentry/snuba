import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Mapping, MutableSequence, Optional, Sequence, Tuple

from snuba import environment, util
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.optimize_scheduler import OptimizeScheduler
from snuba.optimize_tracker import NoOptimizedStateException, OptimizedPartitionTracker
from snuba.settings import (
    OPTIMIZE_BASE_SLEEP_TIME,
    OPTIMIZE_MERGE_MAX_CONCURRENT_JOBS,
    OPTIMIZE_MERGE_MIN_ELAPSED_CUTTOFF_TIME,
    OPTIMIZE_MERGE_SIZE_CUTOFF,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.optimize")
metrics = MetricsWrapper(environment.metrics, "optimize")


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

    partitions = get_partitions_to_optimize(
        clickhouse, storage, database, table, before
    )

    partition_names = [partition.name for partition in partitions]

    optimize_partitions(
        clickhouse=clickhouse,
        database=database,
        table=table,
        partitions=partition_names,
    )

    return len(partitions)


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
    optimize_scheduler = OptimizeScheduler(parallel=parallel)

    try:
        partitions_to_optimize = tracker.get_partitions_to_optimize()
    except NoOptimizedStateException:
        # We don't have any recorded state of partitions needing optimization
        # for today. So we need to build it.
        partitions = get_partitions_to_optimize(
            clickhouse, storage, database, table, before
        )

        if len(partitions) == 0:
            logger.info("No partitions need optimization")
            return 0
        partition_names = [partition.name for partition in partitions]
        tracker.update_all_partitions(partition_names)
        partitions_to_optimize = set(partition_names)

    if len(partitions_to_optimize) == 0:
        logger.info("No partitions need optimization")
        return 0

    logger.info(f"All partitions list: {partitions_to_optimize}")

    optimize_partition_runner(
        clickhouse=clickhouse,
        database=database,
        table=table,
        partitions=list(partitions_to_optimize),
        tracker=tracker,
        scheduler=optimize_scheduler,
        clickhouse_host=clickhouse_host,
    )

    metrics.timing(
        "optimized_all_parts",
        time.time() - start,
        tags=_get_metrics_tags(table, clickhouse_host),
    )
    return len(partitions_to_optimize)


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
    partition_format = schema.get_partition_format()
    assert partition_format is not None

    parts = [
        util.decode_part_str(part, partition_format)
        for part, count in active_parts.results
    ]

    if before:
        parts = [
            p for p in parts if (p.date + timedelta(days=6 - p.date.weekday())) < before
        ]

    return parts


def get_current_merging_partitions_info(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
) -> Sequence[util.MergeInfo]:
    """
    Returns a dictionary of partitions that are currently part of large merged. Merges
    are considered large if they are longer than OPTIMIZE_MERGE_MIN_ELAPSED_CUTTOFF_TIME
    or if they are larger than OPTIMIZE_MERGE_MIN_PARTS_CUTTOFF_TIME.
    """
    merging_parts = clickhouse.execute(
        """
        SELECT
            result_part_name,
            elapsed,
            progress,      -- get the slowest merge combination possible
            total_size_bytes_compressed
        FROM system.merges
        WHERE database = %(database)s
        AND (
            total_size_bytes_compressed > %(max_merge_size)d
            OR elapsed > %(min_merge_elapsed_time)f
            )
        AND table = %(table)s
        ORDER BY elapsed DESC
        """,
        {
            "database": database,
            "table": table,
            "max_merge_size": OPTIMIZE_MERGE_SIZE_CUTOFF,
            "min_merge_elapsed_time": OPTIMIZE_MERGE_MIN_ELAPSED_CUTTOFF_TIME,
        },
    )

    merge_info = []
    for part, elapsed, progress, size in merging_parts.results:
        partition_id = part.split("_")[0]
        merge_info.append(util.MergeInfo(partition_id, part, elapsed, progress, size))

    return merge_info


def optimize_partition_runner(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    partitions: Sequence[str],
    scheduler: OptimizeScheduler,
    tracker: OptimizedPartitionTracker,
    clickhouse_host: str,
) -> None:
    """
    Run optimize jobs in threads based on the optimization scheduler.

    The scheduler provides the schedule for running the optimization. Based
    on the schedule, this method executes the schedule.

    The execution continues until one of the following conditions is met:
    1. There are no more partitions which need optimization.
    2. The final cutoff time is reached. In this case, the scheduler will
    raise an exception which would be propagated to the caller.
    """
    remaining_partitions = partitions

    while remaining_partitions:
        schedule = scheduler.get_next_schedule(remaining_partitions)
        num_threads = len(schedule.partitions)
        logger.info(
            f"Running schedule with cutoff time: "
            f"{schedule.cutoff_time} with {num_threads} threads"
        )
        threads: MutableSequence[threading.Thread] = []
        for i in range(0, num_threads):
            threads.append(
                threading.Thread(
                    target=optimize_partitions,
                    args=(
                        clickhouse,
                        database,
                        table,
                        schedule.partitions[i],
                        schedule.cutoff_time,
                        tracker,
                        clickhouse_host,
                        schedule.start_time_jitter_minutes[i]
                        if schedule.start_time_jitter_minutes
                        else None,
                    ),
                )
            )

            threads[i].start()

        # Wait for all threads to finish. They would finish either because all
        # work is done or because a cutoff time was reached. We won't know the
        # reason why the threads finished.
        for i in range(0, num_threads):
            threads[i].join()

        # If there are still partitions needing optimization then move on to the
        # next bucket with the partitions which still need optimization.
        remaining_partitions = list(tracker.get_partitions_to_optimize())
        if len(remaining_partitions) == 0:
            return


def optimize_partitions(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    partitions: Sequence[str],
    cutoff_time: Optional[datetime] = None,
    tracker: Optional[OptimizedPartitionTracker] = None,
    clickhouse_host: Optional[str] = None,
    start_jitter: Optional[int] = None,
) -> None:
    query_template = """\
        OPTIMIZE TABLE %(database)s.%(table)s
        PARTITION %(partition)s FINAL
    """

    if start_jitter is not None:
        logger.info(
            f"{threading.current_thread().name}: Jittering start time by"
            f" {start_jitter} minutes"
        )
        time.sleep(start_jitter * 60)

    for partition in partitions:
        if cutoff_time is not None and datetime.now() > cutoff_time:
            logger.info(
                f"Optimize job is running past provided cutoff time"
                f" {cutoff_time}. Cancelling.",
            )
            return

        args = {
            "database": database,
            "table": table,
            "partition": partition,
        }

        query = (query_template % args).strip()
        logger.info(f"Optimizing partition: {partition}")

        # Update tracker before running clickhouse.execute since its possible
        # that the connection can get disconnected while the server is still
        # executing the optimization. In that case we don't want to
        # run optimization on the same partition twice.
        if tracker:
            tracker.update_scheduled_partitions(partition)

        # if theres a merge in progress for this partition, wait for it to finish
        while True:
            busy_merging, estimated_sleep_time = is_busy_merging(
                clickhouse, database, table
            )
            if not busy_merging:
                break
            else:
                time.sleep(OPTIMIZE_BASE_SLEEP_TIME + estimated_sleep_time)

        start = time.time()
        clickhouse.execute(query, retryable=False)
        metrics.timing(
            "optimized_part",
            time.time() - start,
            tags=_get_metrics_tags(table, clickhouse_host),
        )


def is_busy_merging(
    clickhouse: ClickhousePool, database: str, table: str
) -> Tuple[bool, float]:
    """
    Returns true and the estimated sleep time if clickhouse is busy with merges in progress
    for the table. Clickhouse is considered busy if
        1. there are more than OPTIMIZE_MERGE_MAX_CONCURRENT_JOBS merges in progress
        2. or there is a merge of size greater than OPTIMIZE_MERGE_SIZE_CUTOFF
    """
    merge_info = get_current_merging_partitions_info(clickhouse, database, table)

    if len(merge_info) > OPTIMIZE_MERGE_MAX_CONCURRENT_JOBS:
        estimated_sleep_time = max(
            merge_info, key=lambda x: x.estimated_time
        ).estimated_time
        logger.info(
            f"too many concurrent merges {len(merge_info)}, sleeping for {estimated_sleep_time}s"
        )
        return True, estimated_sleep_time

    if any(merge.size > OPTIMIZE_MERGE_SIZE_CUTOFF for merge in merge_info):
        estimated_sleep_time = max(
            merge_info, key=lambda x: x.estimated_time
        ).estimated_time
        logger.info("large ongoing merge, sleeping for {estimated_sleep_time}s")
        return True, estimated_sleep_time

    return False, 0
