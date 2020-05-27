import sys
from typing import Optional

import click

from snuba.environment import setup_logging
from snuba.migrations.migrate import logger, run
from snuba.util import local_dataset_mode


@click.command()
@click.option("--log-level", help="Logging level to use.")
def migrate(*, log_level: Optional[str] = None) -> None:
    setup_logging(log_level)

    if not local_dataset_mode():
        logger.error("The migration tool can only work on local dataset mode.")
        sys.exit(1)

    run()
