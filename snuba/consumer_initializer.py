from batching_kafka_consumer import AbstractBatchWorker, BatchingKafkaConsumer
from confluent_kafka import Producer
from typing import Any, Sequence

from snuba.datasets import Dataset
from snuba.consumer import ConsumerWorker
from snuba import settings
from snuba import util


def initialize_consumer(
    dataset: Dataset,
    dataset_name: str,
    raw_topic: str,
    replacements_topic: str,
    max_batch_size: int,
    max_batch_time_ms: int,
    bootstrap_server: Sequence[str],
    group_id: str,
    commit_log_topic: str,
    auto_offset_reset: str,
    queued_max_messages_kbytes: int,
    queued_min_messages: int,
    dogstatsd_host: str,
    dogstatsd_port: int
) -> BatchingKafkaConsumer:
    if not bootstrap_server:
        bootstrap_server = settings.DEFAULT_DATASET_BROKERS.get(
            dataset_name,
            settings.DEFAULT_BROKERS,
        )

    raw_events_topic = raw_topic or dataset.get_default_topic()
    replacements_topic = replacements_topic or dataset.get_default_replacement_topic()
    commit_log_topic = commit_log_topic or dataset.get_default_commit_log_topic()

    producer = Producer({
        'bootstrap.servers': ','.join(bootstrap_server),
        'partitioner': 'consistent',
        'message.max.bytes': 50000000,  # 50MB, default is 1MB
    })

    metrics = util.create_metrics(
        dogstatsd_host, dogstatsd_port, 'snuba.consumer',
        tags=[
            "group:%s" % group_id,
            "dataset:%s" % dataset_name,
        ]
    )

    return BatchingKafkaConsumer(
        raw_events_topic,
        worker=ConsumerWorker(
            dataset,
            producer=producer,
            replacements_topic=replacements_topic,
            metrics=metrics
        ),
        max_batch_size=max_batch_size,
        max_batch_time=max_batch_time_ms,
        metrics=metrics,
        bootstrap_servers=bootstrap_server,
        group_id=group_id,
        producer=producer,
        commit_log_topic=commit_log_topic,
        auto_offset_reset=auto_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
    )
