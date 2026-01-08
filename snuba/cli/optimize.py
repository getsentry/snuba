from datetime import UTC, timedelta
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
    "--clickhouse-secure",
    type=bool,
    default=False,
    help="If true, an encrypted connection will be used",
)
@click.option(
    "--clickhouse-ca-certs",
    type=str,
    default=None,
    help="An optional path to certificates directory.",
)
@click.option(
    "--clickhouse-verify",
    type=bool,
    default=False,
    help="Verify ClickHouse SSL cert.",
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
    "default_parallel_threads",
    type=click.IntRange(1, 2),
    default=1,
    help="Default parallel threads",
)
@click.option(
    "--divide-partitions",
    type=click.IntRange(1, 2),
    default=1,
    help="Divide partitions into N groups",
)
def optimize(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    clickhouse_secure: bool,
    clickhouse_ca_certs: Optional[str],
    clickhouse_verify: Optional[bool],
    storage_name: str,
    default_parallel_threads: int,
    log_level: Optional[str] = None,
    divide_partitions: int,
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

    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

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
            clickhouse_secure,
            clickhouse_ca_certs,
            clickhouse_verify,
            send_receive_timeout=ClickhouseClientSettings.OPTIMIZE.value.timeout,
        )
    elif not storage.get_cluster().is_single_node():
        raise click.ClickException("Provide Clickhouse host and port for optimize")
    else:
        connection = storage.get_cluster().get_query_connection(ClickhouseClientSettings.OPTIMIZE)
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
        default_parallel_threads=default_parallel_threads,
        clickhouse_host=clickhouse_host,
        tracker=tracker,
        before=today,
        divide_partitions_count=divide_partitions,
    )

    tracker.delete_all_states()
    logger.info("Optimized %s partitions on %s" % (num_dropped, clickhouse_host))
