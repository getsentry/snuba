import logging
from typing import Optional

import click

from snuba import settings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_cdc_storage, CDC_STORAGES
from snuba.environment import setup_logging, setup_sentry
from snuba.snapshots.postgres_snapshot import PostgresSnapshot
from snuba.writer import BufferedWriterWrapper


@click.command()
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice([storage_key.value for storage_key in CDC_STORAGES.keys()]),
    help="The CDC storage to bulk load",
)
@click.option(
    "--source",
    help="Source of the dump. Depending on the storage it may have different meaning.",
)
@click.option("--dest-table", help="Clickhouse destination table.")
@click.option("--log-level", help="Logging level to use.")
def bulk_load(
    *, storage_name: str, dest_table: str, source: str, log_level: Optional[str] = None,
) -> None:
    setup_logging(log_level)
    setup_sentry()

    logger = logging.getLogger("snuba.load-snapshot")
    logger.info(
        "Start bulk load process for storage %s, from source %s", storage_name, source
    )

    storage = get_cdc_storage(StorageKey(storage_name))
    table_writer = storage.get_table_writer()

    # TODO: Have a more abstract way to load sources if/when we support more than one.
    snapshot_source = PostgresSnapshot.load(
        product=settings.SNAPSHOT_LOAD_PRODUCT, path=source,
    )

    loader = table_writer.get_bulk_loader(
        snapshot_source,
        storage.get_postgres_table(),
        dest_table,
        storage.get_row_processor(),
    )
    # TODO: see whether we need to pass options to the writer
    writer = BufferedWriterWrapper(
        table_writer.get_bulk_writer(table_name=dest_table),
        settings.BULK_CLICKHOUSE_BUFFER,
    )

    loader.load(writer)
