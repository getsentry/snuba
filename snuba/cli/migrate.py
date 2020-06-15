from typing import Optional

import click

from snuba.environment import setup_logging
from snuba.migrations.migrate import run


@click.command()
@click.option("--log-level", help="Logging level to use.")
def migrate(*, log_level: Optional[str] = None) -> None:
    setup_logging(log_level)

    run()
