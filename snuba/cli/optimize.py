import logging

import click

from snuba import settings, datasets
from snuba.datasets.factory import get_dataset


@click.command()
@click.option('--clickhouse-host', default=settings.CLICKHOUSE_HOST,
              help='Clickhouse server to write to.')
@click.option('--clickhouse-port', default=settings.CLICKHOUSE_PORT, type=int,
              help='Clickhouse native port to write to.')
@click.option('--database', default='default',
              help='Name of the database to target.')
@click.option('--dataset', default='events', type=click.Choice(['events']),
              help='The dataset to target')
@click.option('--timeout', default=10000, type=int,
              help='Clickhouse connection send/receive timeout, must be long enough for OPTIMIZE to complete.')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def optimize(clickhouse_host, clickhouse_port, database, dataset, timeout, log_level):
    from datetime import datetime
    from snuba.clickhouse import ClickhousePool
    from snuba.optimize import run_optimize, logger

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')

    dataset = get_dataset(dataset)
    table = dataset.get_schema().get_local_table_name()

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    clickhouse = ClickhousePool(clickhouse_host, clickhouse_port, send_receive_timeout=timeout)
    num_dropped = run_optimize(clickhouse, database, table, before=today)
    logger.info("Optimized %s partitions on %s" % (num_dropped, clickhouse_host))
