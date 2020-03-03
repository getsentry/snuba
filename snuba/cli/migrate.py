import sys
from typing import Optional

import click
from clickhouse_driver import Client

from snuba import settings
from snuba.datasets.factory import DATASET_NAMES, get_dataset
from snuba.environment import setup_logging
from snuba.util import local_dataset_mode


@click.command()
@click.option("--log-level", help="Logging level to use.")
@click.option(
    "--dataset",
    "dataset_name",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
def migrate(
    *, log_level: Optional[str] = None, dataset_name: Optional[str] = None
) -> None:
    from snuba.migrations.migrate import logger, run

    setup_logging(log_level)

    if not local_dataset_mode():
        logger.error("The migration tool can only work on local dataset mode.")
        sys.exit(1)

    dataset_names = [dataset_name] if dataset_name else DATASET_NAMES
    for name in dataset_names:
        dataset = get_dataset(name)
        logger.info("Migrating dataset %s", name)

        clickhouse = Client(
            host=settings.CLICKHOUSE_HOST, port=settings.CLICKHOUSE_PORT,
        )

        run(clickhouse, dataset)
