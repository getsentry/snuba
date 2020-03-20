import logging
from typing import Optional

import click

from snuba import settings
from snuba.datasets.factory import DATASET_NAMES, enforce_table_writer, get_dataset
from snuba.environment import setup_logging, setup_sentry
from snuba.snapshots.postgres_snapshot import PostgresSnapshot
from snuba.writer import BufferedWriterWrapper


@click.command()
@click.option(
    "--dataset",
    "dataset_name",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to bulk load",
)
@click.option(
    "--source",
    help="Source of the dump. Depending on the dataset it may have different meaning.",
)
@click.option("--dest-table", help="Clickhouse destination table.")
@click.option("--log-level", help="Logging level to use.")
def bulk_load(
    *,
    dataset_name: Optional[str],
    dest_table: Optional[str],
    source: Optional[str],
    log_level: Optional[str] = None,
) -> None:
    setup_logging(log_level)
    setup_sentry()

    logger = logging.getLogger("snuba.load-snapshot")
    logger.info(
        "Start bulk load process for dataset %s, from source %s", dataset_name, source
    )
    dataset = get_dataset(dataset_name)

    # TODO: Have a more abstract way to load sources if/when we support more than one.
    snapshot_source = PostgresSnapshot.load(
        product=settings.SNAPSHOT_LOAD_PRODUCT, path=source,
    )

    loader = enforce_table_writer(dataset).get_bulk_loader(snapshot_source, dest_table)
    # TODO: see whether we need to pass options to the writer
    writer = BufferedWriterWrapper(
        enforce_table_writer(dataset).get_bulk_writer(table_name=dest_table),
        settings.BULK_CLICKHOUSE_BUFFER,
    )

    loader.load(writer)
