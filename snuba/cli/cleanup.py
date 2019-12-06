import logging

import click

from snuba import settings
from snuba.datasets.factory import enforce_table_writer, get_dataset, DATASET_NAMES


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
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
def cleanup(
    *,
    clickhouse_host: str,
    clickhouse_port: int,
    dry_run: bool,
    database: str,
    dataset_name: str,
    log_level: str,
) -> None:
    """
    Deletes stale partitions for ClickHouse tables
    """

    from snuba.cleanup import run_cleanup, logger
    from snuba.clickhouse.native import ClickhousePool

    dataset = get_dataset(dataset_name)
    table = enforce_table_writer(dataset).get_schema().get_local_table_name()

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format="%(asctime)s %(message)s"
    )

    clickhouse = ClickhousePool(clickhouse_host, clickhouse_port)
    num_dropped = run_cleanup(clickhouse, database, table, dry_run=dry_run)
    logger.info("Dropped %s partitions on %s" % (num_dropped, clickhouse_host))
