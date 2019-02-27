import logging
import sys

import click

from snuba import settings


@click.command()
@click.option('--clickhouse-server', multiple=True,
              help='Clickhouse server to cleanup.')
@click.option('--dry-run', type=bool, default=True,
              help="If true, only print which partitions would be dropped.")
@click.option('--database', default='default',
              help='Name of the database to target.')
@click.option('--table',
              help='Name of the table to target.')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def cleanup(clickhouse_server, dry_run, database, table, log_level):
    from snuba.cleanup import run_cleanup, logger
    from snuba.clickhouse import ClickhousePool

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')

    if not table:
        logger.error("Must provide a table name.")
        sys.exit(1)
    if not clickhouse_server:
        logger.error("Must provide at least one Clickhouse server.")
        sys.exit(1)

    for server in clickhouse_server:
        clickhouse = ClickhousePool(server.split(':')[0], port=int(server.split(':')[1]))
        num_dropped = run_cleanup(clickhouse, database, table, dry_run=dry_run)
        logger.info("Dropped %s partitions on %s" % (num_dropped, server))
