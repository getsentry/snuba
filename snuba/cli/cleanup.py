from typing import Optional

import click

from snuba.clickhouse.config import ClickhouseConnectionConfig
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.environment import setup_logging


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
    from snuba.clickhouse.pool import ClickhousePool

    if bool(clickhouse_host) ^ bool(clickhouse_port):
        raise click.ClickException("Provide both clickhouse_host and clickhouse_port, or neither")

    if clickhouse_host and clickhouse_port:
        clickhouse_connection_config = ClickhouseConnectionConfig(
            host=clickhouse_host,
            port=clickhouse_port
        )
    else:
        clickhouse_connection_config = None

    dataset = get_dataset(dataset_name, clickhouse_connection_config)
    table = enforce_table_writer(dataset).get_schema().get_local_table_name()

    clickhouse = ClickhousePool(clickhouse_host, clickhouse_port)
    num_dropped = run_cleanup(clickhouse, database, table, dry_run=dry_run)
    logger.info("Dropped %s partitions on %s" % (num_dropped, clickhouse_host))
