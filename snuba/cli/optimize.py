from datetime import timedelta
from typing import Optional

import click

from snuba import settings
from snuba.clickhouse.optimize.optimize import run_optimize_cron_job
from snuba.clickhouse.optimize.optimize_tracker import OptimizedPartitionTracker
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.redis import RedisClientKey, get_redis_client


@click.command()
@click.option(
    "--clickhouse-host",
    help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port",
    type=int,
    help="Clickhouse native port to write to.",
)
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(["errors"]),
    help="The storage to target",
    required=True,
)
@click.option("--log-level", help="Logging level to use.")
@click.option(
    "--parallel",
    type=click.IntRange(1, 2),
    default=1,
    help="Run parallel optimizations",
)
def optimize(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    storage_name: str,
    parallel: int,
    log_level: Optional[str] = None,
) -> None:
    from datetime import datetime

    from snuba.clickhouse.native import ClickhousePool
    from snuba.clickhouse.optimize.optimize import logger

    setup_logging(log_level)
    setup_sentry()

    storage: ReadableTableStorage

    storage_key = StorageKey(storage_name)
    storage = get_storage(storage_key)

    (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    database = storage.get_cluster().get_database()

    # TODO: In distributed mode, optimize currently must be run once for each node
    # with the host and port of that node provided via the CLI. In the future,
    # passing this information won't be necessary, and running this command once
    # will ensure that optimize is performed on all of the individual nodes for
    # that cluster.
    if clickhouse_host and clickhouse_port:
        connection = ClickhousePool(
            clickhouse_host,
            clickhouse_port,
            clickhouse_user,
            clickhouse_password,
            database,
            send_receive_timeout=ClickhouseClientSettings.OPTIMIZE.value.timeout,
        )
    elif not storage.get_cluster().is_single_node():
        raise click.ClickException("Provide Clickhouse host and port for optimize")
    else:
        connection = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.OPTIMIZE
        )
        clickhouse_host = storage.get_cluster().get_host()
        clickhouse_port = storage.get_cluster().get_port()

    # Adding 10 minutes to the current time before finding the midnight time
    # to ensure this keeps working even if the system clock of the host that
    # starts the pod is slightly ahead of the system clock of the host running
    # the job. This prevents us from getting the wrong midnight.
    last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # add 1 hour to make the redis TTL past the optimize job cuttoff time
    cutoff_time = last_midnight + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME)
    redis_expire_time = cutoff_time + timedelta(hours=1)
    logger.info(f"Cutoff time: {str(cutoff_time)}")
    logger.info(f"redis_expire_time time: {str(redis_expire_time)}")

    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    table = schema.get_local_table_name()
    tracker = OptimizedPartitionTracker(
        redis_client=get_redis_client(RedisClientKey.OPTIMIZE),
        host=clickhouse_host,
        port=clickhouse_port,
        database=database,
        table=table,
        expire_time=redis_expire_time,
    )

    num_dropped = run_optimize_cron_job(
        clickhouse=connection,
        storage=storage,
        database=database,
        parallel=parallel,
        clickhouse_host=clickhouse_host,
        tracker=tracker,
        before=today,
    )

    tracker.delete_all_states()
    logger.info("Optimized %s partitions on %s" % (num_dropped, clickhouse_host))
