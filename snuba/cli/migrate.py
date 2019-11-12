import sys
import click
from clickhouse_driver import Client

from snuba import settings
from snuba.datasets.factory import get_dataset
from snuba.util import local_dataset_mode


@click.command()
def migrate():
    from snuba.migrate import logger, run
    # TODO: this only supports one dataset so far. More work is needed for the others.
    dataset = get_dataset('events')

    if not local_dataset_mode():
        logger.error("The migration tool can only work on local dataset mode.")
        sys.exit(1)

    clickhouse = Client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
    )

    run(clickhouse, dataset)
