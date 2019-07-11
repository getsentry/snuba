import logging
import signal

import click
from confluent_kafka import Producer

from snuba import settings
from snuba.datasets.factory import get_dataset


@click.command()
@click.option('--raw-events-topic', default=None,
              help='Topic to consume raw events from.')
@click.option('--replacements-topic', default=None,
              help='Topic to produce replacement messages info.')
@click.option('--commit-log-topic', default=None,
              help='Topic for committed offsets to be written to, triggering post-processing task(s)')
@click.option('--consumer-group', default=None,
              help='Consumer group use for consuming the raw events topic.')
@click.option('--bootstrap-server', default=[], multiple=True,
              help='Kafka bootstrap server to use.')
@click.option('--clickhouse-server', default=settings.CLICKHOUSE_SERVER,
              help='Clickhouse server to write to.')
@click.option('--dataset', default='events', type=click.Choice(['events', 'groupedmessage']),
              help='The dataset to target')
@click.option('--max-batch-size', default=None,
              help='Max number of messages to batch in memory before writing to Kafka.')
@click.option('--max-batch-time-ms', default=None,
              help='Max length of time to buffer messages in memory before writing to Kafka.')
@click.option('--auto-offset-reset', default='error', type=click.Choice(['error', 'earliest', 'latest']),
              help='Kafka consumer auto offset reset.')
@click.option('--queued-max-messages-kbytes', default=None, type=int,
              help='Maximum number of kilobytes per topic+partition in the local consumer queue.')
@click.option('--queued-min-messages', default=None, type=int,
              help='Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.')
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
@click.option('--dogstatsd-host', default=settings.DOGSTATSD_HOST, help='Host to send DogStatsD metrics to.')
@click.option('--dogstatsd-port', default=settings.DOGSTATSD_PORT, type=int, help='Port to send DogStatsD metrics to.')
def consumer(raw_events_topic, replacements_topic, commit_log_topic, consumer_group,
             bootstrap_server, clickhouse_server, dataset, max_batch_size, max_batch_time_ms,
             auto_offset_reset, queued_max_messages_kbytes, queued_min_messages, log_level,
             dogstatsd_host, dogstatsd_port):

    import sentry_sdk
    from snuba import util
    from snuba.clickhouse import ClickhousePool
    from batching_kafka_consumer import BatchingKafkaConsumer
    from snuba.consumer import ConsumerWorker

    sentry_sdk.init(dsn=settings.SENTRY_DSN)

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s')
    dataset_name = dataset
    dataset = get_dataset(dataset_name)

    dataset_config = settings.load_dataset_settings(
        dataset=dataset_name,
        override=settings.deep_copy_and_merge(
            default={},
            override={
                'consumer': {
                    'kafka_cluster_override': {
                        'brokers': bootstrap_server,
                        'max_batch_size': max_batch_size,
                        'max_batch_time_ms': max_batch_time_ms,
                        'queued_max_message_kbytes': queued_max_messages_kbytes,
                        'queued_min_messages': queued_min_messages,
                    },
                    'message_topic': {
                        'name': raw_events_topic,
                        'consumer_group': consumer_group,
                    },
                    'commit_log_topic': commit_log_topic,
                    'replacement_topic': replacements_topic,
                }
            },
            skip_null=True,
        )
    )

    consumer_config = dataset_config.consumer
    metrics = util.create_metrics(
        dogstatsd_host, dogstatsd_port, 'snuba.consumer',
        tags=[
            "group:%s" % consumer_config.message_topic.consumer_group,
            "dataset:%s" % dataset_name,
        ]
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
        'bootstrap.servers': ','.join(consumer_config.kafka_cluster.brokers),
        'partitioner': 'consistent',
        'message.max.bytes': 50000000,  # 50MB, default is 1MB
    })

    consumer = BatchingKafkaConsumer(
        consumer_config.message_topic.name,
        worker=ConsumerWorker(
            clickhouse,
            dataset,
            producer=producer,
            replacements_topic=consumer_config.replacements_topic,
            metrics=metrics
        ),
        max_batch_size=consumer_config.kafka_cluster.max_batch_size,
        max_batch_time=consumer_config.kafka_cluster.max_batch_time_ms,
        metrics=metrics,
        bootstrap_servers=consumer_config.kafka_cluster.brokers,
        group_id=consumer_config.message_topic.consumer_group,
        producer=producer,
        commit_log_topic=consumer_config.commit_log_topic,
        auto_offset_reset=auto_offset_reset,
    )

    def handler(signum, frame):
        consumer.signal_shutdown()

    signal.signal(signal.SIGINT, handler)

    consumer.run()
