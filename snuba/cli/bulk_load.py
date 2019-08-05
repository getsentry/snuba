import logging

import click

from snuba import settings
from snuba.datasets.factory import get_dataset
from snuba.snapshots.postgres_snapshot import PostgresSnapshot


@click.command()
@click.option('--dataset', type=click.Choice(['events', 'groupedmessage', 'outcomes']),
              help='The dataset to bulk load')
@click.option('--source',
              help='Source of the dump. Depending on the dataset it may have different meaning.')
@click.option('--dest-table',
              help='Clickhouse destination table.')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def bulk_load(dataset, dest_table, source, log_level):
    import sentry_sdk

    sentry_sdk.init(dsn=settings.SENTRY_DSN)
    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')

    logger = logging.getLogger('load-snapshot')
    logger.info("Start bulk load process for dataset %s, from source %s", dataset, source)
    dataset = get_dataset(dataset)

    # TODO: Have a more abstract way to load sources if/when we support more than one.
    snapshot_source = PostgresSnapshot.load(
        path=source,
        product=settings.SNAPSHOT_LOAD_PRODUCT,
    )

    loader = dataset.get_bulk_loader(snapshot_source, dest_table)

    loader.load()
