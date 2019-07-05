import logging
import sys

import click

from snuba import settings
from snuba.datasets.factory import get_dataset


@click.command()
@click.option('--clickhouse-host', default=settings.CLICKHOUSE_HOST,
              help='Clickhouse server to write to.')
@click.option('--clickhouse-port', default=settings.CLICKHOUSE_PORT, type=int,
              help='Clickhouse native port to write to.')
@click.option('--dry-run', type=bool, default=True,
              help="If true, only print which partitions would be dropped.")
@click.option('--database', default='default',
              help='Name of the database to target.')
@click.option('--dataset', default='events', type=click.Choice(['events']),
              help='The dataset to target')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def cleanup(clickhouse_host, clickhouse_port, dry_run, database, dataset, log_level):
    from snuba.cleanup import run_cleanup, logger
    from snuba.clickhouse import ClickhousePool

    dataset = get_dataset(dataset)
    table = dataset.get_schema().get_local_table_name()

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')

    if not clickhouse_server:
        logger.error("Must provide at least one Clickhouse server.")
        sys.exit(1)

    for server in clickhouse_server:
        clickhouse = ClickhousePool(clickhouse_host, clickhouse_port)
        num_dropped = run_cleanup(clickhouse, database, table, dry_run=dry_run)
        logger.info("Dropped %s partitions on %s" % (num_dropped, server))
