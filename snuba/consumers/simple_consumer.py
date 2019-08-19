from confluent_kafka import Consumer, Message
from abc import ABC, abstractmethod
from enum import Enum
from typing import Sequence

import logging

logger = logging.getLogger('snuba.kafka-consumer')


def create_simple_consumer(
    topics: str,
    bootstrap_servers: Sequence[str],
    group_id: str,
    auto_offset_reset: str,
) -> Consumer:
    consumer_config = {
        'enable.auto.commit': False,
        'bootstrap.servers': ','.join(bootstrap_servers),
        'group.id': group_id,
        'enable.partition.eof': 'false',
        'default.topic.config': {
            'auto.offset.reset': auto_offset_reset,
        },
    }

    consumer = Consumer(consumer_config)

    def on_partitions_assigned(consumer, partitions):
        logger.info("New partitions assigned: %r", partitions)

    def on_partitions_revoked(consumer, partitions):
        logger.info("Partitions revoked: %r", partitions)

    consumer.subscribe(
        topics,
        on_assign=on_partitions_assigned,
        on_revoke=on_partitions_revoked,
    )

    return consumer
