import logging
import signal

import click
from confluent_kafka import Producer

from snuba import settings


@click.command()
@click.option('--bootstrap-server', default=settings.DEFAULT_BROKERS, multiple=True,
              help='Kafka bootstrap server to use.')
@click.option('--clickhouse-server', default=settings.CLICKHOUSE_SERVER,
              help='Clickhouse server to write to.')
@click.option('--dataset', default='events', type=click.Choice(settings.DATASETS.keys()),
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
def consumer(bootstrap_server, clickhouse_server, dataset, max_batch_size, max_batch_time_ms,
             auto_offset_reset, queued_max_messages_kbytes, queued_min_messages, log_level,
             dogstatsd_host, dogstatsd_port):

    import sentry_sdk
    from snuba import util
    from snuba.clickhouse import ClickhousePool
    from batching_kafka_consumer import BatchingKafkaConsumer
    from snuba.consumer import ConsumerWorker

    sentry_sdk.init(dsn=settings.SENTRY_DSN)
    dataset = settings.get_dataset(dataset)

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')
    metrics = util.create_metrics(
        dogstatsd_host, dogstatsd_port, 'snuba.consumer', tags=["group:%s" % dataset.PROCESSOR.MESSAGE_CONSUMER_GROUP]
    )

    clickhouse = ClickhousePool(
        host=clickhouse_server.split(':')[0],
        port=int(clickhouse_server.split(':')[1]),
        client_settings={
            'load_balancing': 'in_order',
            'insert_distributed_sync': True,
        },
        metrics=metrics
    )

    producer = Producer({
        'bootstrap.servers': ','.join(bootstrap_server),
        'partitioner': 'consistent',
        'message.max.bytes': 50000000,  # 50MB, default is 1MB
    })

    consumer = BatchingKafkaConsumer(
        dataset.PROCESSOR.MESSAGE_TOPIC,
        worker=ConsumerWorker(
            clickhouse, dataset,
            producer=producer,
            replacements_topic=dataset.PROCESSOR.REPLACEMENTS_TOPIC,
            metrics=metrics
        ),
        max_batch_size=max_batch_size,
        max_batch_time=max_batch_time_ms,
        metrics=metrics,
        bootstrap_servers=bootstrap_server,
        group_id=dataset.PROCESSOR.MESSAGE_CONSUMER_GROUP,
        producer=producer,
        commit_log_topic=dataset.PROCESSOR.COMMIT_LOG_TOPIC,
        auto_offset_reset=auto_offset_reset,
    )

    def handler(signum, frame):
        consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)

    consumer.run()
