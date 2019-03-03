import logging
import sys
import click
from clickhouse_driver import Client

from snuba import settings


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def migrate(log_level):
    from snuba.migrate import logger, run

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s'
    )

    if settings.CLICKHOUSE_TABLE != 'dev':
        logger.error(
            "The migration tool is only intended for local development environment."
        )
        sys.exit(1)

    host, port = settings.CLICKHOUSE_SERVER.split(':')
    clickhouse = Client(host=host, port=port)

    run(clickhouse, settings.CLICKHOUSE_TABLE)
