from typing import Optional

import click

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.environment import setup_logging
from snuba.util import local_dataset_mode


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
@click.option("--log-level", help="Logging level to use.")
def optimize(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    database: str,
    dataset_name: str,
    log_level: Optional[str] = None,
) -> None:
    from datetime import datetime
    from snuba.clickhouse.native import ClickhousePool
    from snuba.optimize import OPTIMIZE_SEND_RECEIVE_TIMEOUT, run_optimize, logger

    setup_logging(log_level)

    dataset = get_dataset(dataset_name)
    table = enforce_table_writer(dataset).get_schema().get_local_table_name()

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    if clickhouse_host and clickhouse_port:
        clickhouse_connections = [
            ClickhousePool(
                clickhouse_host,
                clickhouse_port,
                send_receive_timeout=OPTIMIZE_SEND_RECEIVE_TIMEOUT,
            )
        ]
    elif not local_dataset_mode():
        raise click.ClickException("Provide Clickhouse host and port for optimize")
    else:
        # In local mode, we run optimize on each cluster relevant to the provided
        # dataset using the cluster's host/port configuration.
        clickhouse_connections = list(
            set(
                storage.get_cluster().get_connection(ClickhouseClientSettings.OPTIMIZE)
                for storage in dataset.get_all_storages()
            )
        )

    for connection in clickhouse_connections:
        num_dropped = run_optimize(connection, database, table, before=today)
        logger.info("Optimized %s partitions on %s" % (num_dropped, clickhouse_host))
