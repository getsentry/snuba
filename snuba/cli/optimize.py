from typing import Optional

import click

from snuba.clusters.cluster import CLUSTERS
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.environment import setup_logging


@click.command()
@click.option(
    "--clickhouse-host", help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port", type=int, help="Clickhouse native port to write to.",
)
@click.option("--database", default="default", help="Name of the database to target.")
@click.option(
    "--dataset",
    "dataset_name",
    default="events",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
@click.option(
    "--timeout",
    default=10000,
    type=int,
    help="Clickhouse connection send/receive timeout, must be long enough for OPTIMIZE to complete.",
)
@click.option("--log-level", help="Logging level to use.")
def optimize(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    database: str,
    dataset_name: str,
    timeout: int,
    log_level: Optional[str] = None,
) -> None:
    from datetime import datetime
    from snuba.clickhouse.native import ClickhousePool
    from snuba.optimize import run_optimize, logger

    setup_logging(log_level)

    dataset = get_dataset(dataset_name)
    table = enforce_table_writer(dataset).get_schema().get_local_table_name()

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    if bool(clickhouse_host) ^ bool(clickhouse_port):
        raise click.ClickException("Provide both Clickhouse host and port or neither")

    # If Clickhouse host and port values are provided, run optimize on that host,
    # otherwise run optimize on each registered cluster.
    if clickhouse_host and clickhouse_port:
        clickhouse_connections = [
            ClickhousePool(
                clickhouse_host, clickhouse_port, send_receive_timeout=timeout
            )
        ]
    else:
        clickhouse_connections = [
            cluster.get_connection(send_receive_timeout=timeout) for cluster in CLUSTERS
        ]

    for connection in clickhouse_connections:
        num_dropped = run_optimize(connection, database, table, before=today)
        logger.info("Optimized %s partitions on %s" % (num_dropped, clickhouse_host))
