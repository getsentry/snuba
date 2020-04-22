from typing import Optional

import click

from snuba import settings
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.environment import setup_logging


@click.command()
@click.option(
    "--clickhouse-host",
    default=settings.CLICKHOUSE_HOST,
    help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port",
    default=settings.CLICKHOUSE_PORT,
    type=int,
    help="Clickhouse native port to write to.",
)
@click.option(
    "--clickhouse-user",
    default=settings.CLICKHOUSE_USER,
    type=str,
    help="Clickhouse username.",
)
@click.option(
    "--clickhouse-pass",
    default=settings.CLICKHOUSE_PASS,
    type=str,
    help="Clickhouse password.",
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
    clickhouse_host: str,
    clickhouse_port: int,
    clickhouse_user: str,
    clickhouse_pass: str,
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

    clickhouse = ClickhousePool(
        clickhouse_host, clickhouse_port, clickhouse_user, clickhouse_pass
    )
    num_dropped = run_cleanup(clickhouse, database, table, dry_run=dry_run)
    logger.info("Dropped %s partitions on %s" % (num_dropped, clickhouse_host))
