from typing import Optional

import click

from snuba.clusters.cluster import ClickhouseClientSettings, CLUSTERS
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.environment import setup_logging


@click.command()
@click.option(
    "--clickhouse-host", help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port", type=int, help="Clickhouse native port to write to.",
)
@click.option(
    "--dry-run",
    type=bool,
    default=True,
    help="If true, only print which partitions would be dropped.",
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
def cleanup(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    dry_run: bool,
    database: str,
    dataset_name: str,
    log_level: Optional[str] = None,
) -> None:
    """
    Deletes stale partitions for ClickHouse tables
    """

    setup_logging(log_level)

    from snuba.cleanup import run_cleanup, logger
    from snuba.clickhouse.native import ClickhousePool

    dataset = get_dataset(dataset_name)
    table = enforce_table_writer(dataset).get_schema().get_local_table_name()

    if bool(clickhouse_host) ^ bool(clickhouse_port):
        raise click.ClickException("Provide both Clickhouse host and port or neither")

    # If Clickhouse host and port values are provided, run cleanup on that host,
    # otherwise run cleanup on each registered cluster.
    if clickhouse_host and clickhouse_port:
        clickhouse_connections = [ClickhousePool(clickhouse_host, clickhouse_port)]
    else:
        clickhouse_connections = [
            cluster.get_connection(ClickhouseClientSettings.READWRITE)
            for cluster in CLUSTERS
        ]

    for connection in clickhouse_connections:
        num_dropped = run_cleanup(connection, database, table, dry_run=dry_run)
        logger.info("Dropped %s partitions on %s" % (num_dropped, clickhouse_host))
