import logging
import signal

import click

from snuba import settings


@click.command()
@click.option('--replacements-topic', default='event-replacements',
              help='Topic to consume replacement messages from.')
@click.option('--consumer-group', default='snuba-replacers',
              help='Consumer group use for consuming the replacements topic.')
@click.option('--bootstrap-server', default=settings.DEFAULT_BROKERS, multiple=True,
              help='Kafka bootstrap server to use.')
@click.option('--clickhouse-server', default=settings.CLICKHOUSE_SERVER,
              help='Clickhouse server to write to.')
@click.option('--dataset', default='events', type=click.Choice(['events']),
              help='The dataset to consume/run replacements for (currently only events supported)')
@click.option('--max-batch-size', default=settings.DEFAULT_MAX_BATCH_SIZE,
              help='Max number of messages to batch in memory before writing to Kafka.')
@click.option('--max-batch-time-ms', default=settings.DEFAULT_MAX_BATCH_TIME_MS,
              help='Max length of time to buffer messages in memory before writing to Kafka.')
@click.option('--auto-offset-reset', default='error', type=click.Choice(['error', 'earliest', 'latest']),
              help='Kafka consumer auto offset reset.')
@click.option('--queued-max-messages-kbytes', default=settings.DEFAULT_QUEUED_MAX_MESSAGE_KBYTES, type=int,
              help='Maximum number of kilobytes per topic+partition in the local consumer queue.')
@click.option('--queued-min-messages', default=settings.DEFAULT_QUEUED_MIN_MESSAGES, type=int,
              help='Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
@click.option('--dogstatsd-host', default=settings.DOGSTATSD_HOST, help='Host to send DogStatsD metrics to.')
@click.option('--dogstatsd-port', default=settings.DOGSTATSD_PORT, type=int, help='Port to send DogStatsD metrics to.')
def replacer(replacements_topic, consumer_group, bootstrap_server, clickhouse_server, dataset,
             max_batch_size, max_batch_time_ms, auto_offset_reset, queued_max_messages_kbytes,
             queued_min_messages, log_level, dogstatsd_host, dogstatsd_port):

    import sentry_sdk
    from snuba import util
    from snuba.clickhouse import ClickhousePool
    from batching_kafka_consumer import BatchingKafkaConsumer
    from snuba.replacer import ReplacerWorker

    sentry_sdk.init(dsn=settings.SENTRY_DSN)

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')
    metrics = util.create_metrics(
        dogstatsd_host, dogstatsd_port, 'snuba.replacer', tags=["group:%s" % consumer_group]
    )

    client_settings = {
        # Replacing existing rows requires reconstructing the entire tuple for each
        # event (via a SELECT), which is a Hard Thing (TM) for columnstores to do. With
        # the default settings it's common for ClickHouse to go over the default max_memory_usage
        # of 10GB per query. Lowering the max_block_size reduces memory usage, and increasing the
        # max_memory_usage gives the query more breathing room.
        'max_block_size': settings.REPLACER_MAX_BLOCK_SIZE,
        'max_memory_usage': settings.REPLACER_MAX_MEMORY_USAGE,
        # Don't use up production cache for the count() queries.
        'use_uncompressed_cache': 0,
    }

    dataset = settings.get_dataset(dataset)
    clickhouse = ClickhousePool(
        host=clickhouse_server.split(':')[0],
        port=int(clickhouse_server.split(':')[1]),
        client_settings=client_settings,
    )

    replacer = BatchingKafkaConsumer(
        replacements_topic,
        worker=ReplacerWorker(clickhouse, dataset, metrics=metrics),
        max_batch_size=max_batch_size,
        max_batch_time=max_batch_time_ms,
        metrics=metrics,
        bootstrap_servers=bootstrap_server,
        group_id=consumer_group,
        producer=None,
        commit_log_topic=None,
        auto_offset_reset=auto_offset_reset,
    )

    def handler(signum, frame):
        replacer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)

    replacer.run()
