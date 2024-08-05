from datetime import datetime, timedelta, timezone
from typing import Optional

import click

from snuba import environment
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "emit-insert-metrics")


@click.command()
@click.option(
    "--clickhouse-host",
    help="Clickhouse server to read insert metrics from.",
)
@click.option(
    "--clickhouse-port",
    type=int,
    help="Clickhouse native port.",
)
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(["generic_metrics_sets_raw"]),
    help="The storage to target",
    required=True,
)
@click.option("--log-level", help="Logging level to use.")
def emit_insert_metrics(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    storage_name: str,
    log_level: Optional[str] = None,
) -> None:

    setup_logging(log_level)
    setup_sentry()

    storage: ReadableTableStorage

    storage_key = StorageKey(storage_name)
    storage = get_storage(storage_key)

    (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()

    database = storage.get_cluster().get_database()

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

    start = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0
    )
    end = start + timedelta(hours=1)

    query = f"""
        SELECT memory_usage, event_time
        FROM system.query_log
        WHERE startsWith(query, 'INSERT')
        AND memory_usage > 0
        AND event_time >= {int(start.timestamp())}
        AND event_time <= {int(end.timestamp())}
        limit 1024
    """
    res = connection.execute(query, retryable=False)

    for max_memory, _ in res.results:
        metrics.gauge(f"{storage_name}-insert-max-memory", max_memory)
