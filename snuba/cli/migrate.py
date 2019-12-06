import logging
import sys
import click
from clickhouse_driver import Client
from typing import Optional

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES
from snuba.util import local_dataset_mode


@click.command()
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
@click.option(
    "--dataset",
    "dataset_name",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
def migrate(*, log_level: str, dataset_name: Optional[str]) -> None:
    from snuba.migrate import logger, run

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format="%(asctime)s %(message)s"
    )
    dataset_names = [dataset_name] if dataset_name else DATASET_NAMES
    for name in dataset_names:
        dataset = get_dataset(name)
        logger.info("Migrating dataset %s", name)
        if not local_dataset_mode():
            logger.error("The migration tool can only work on local dataset mode.")
            sys.exit(1)

        clickhouse = Client(
            host=settings.CLICKHOUSE_HOST, port=settings.CLICKHOUSE_PORT,
        )

        run(clickhouse, dataset)
