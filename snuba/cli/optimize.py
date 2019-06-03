import logging
import sys

import click

from snuba import settings, datasets
from snuba.datasets.factory import get_dataset


@click.command()
@click.option('--clickhouse-server', multiple=True,
              help='Clickhouse server to optimize.')
@click.option('--database', default='default',
              help='Name of the database to target.')
@click.option('--dataset', default='events', type=click.Choice(['events']),
              help='The dataset to target')
@click.option('--timeout', default=10000, type=int,
              help='Clickhouse connection send/receive timeout, must be long enough for OPTIMIZE to complete.')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def optimize(clickhouse_server, database, dataset, timeout, log_level):
    from datetime import datetime
    from snuba.clickhouse import ClickhousePool
    from snuba.optimize import run_optimize, logger

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')

    dataset = get_dataset(dataset)
    table = dataset.get_schema().get_table_name()

    if not clickhouse_server:
        logger.error("Must provide at least one Clickhouse server.")
        sys.exit(1)

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    for server in clickhouse_server:
        clickhouse = ClickhousePool(
            server.split(':')[0], port=int(server.split(':')[1]), send_receive_timeout=timeout
        )
        num_dropped = run_optimize(clickhouse, database, table, before=today)
        logger.info("Optimized %s partitions on %s" % (num_dropped, server))
