from typing import Optional

import click

from snuba.environment import setup_logging
from snuba.migrations.migrate import run
from snuba.migrations.connect import check_clickhouse_connections


@click.command()
@click.option("--log-level", help="Logging level to use.")
def migrate(*, log_level: Optional[str] = None) -> None:
    setup_logging(log_level)

    check_clickhouse_connections()

    run()
