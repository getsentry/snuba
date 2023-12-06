import atexit
import logging
import time
from functools import lru_cache
from typing import Callable, Optional

from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MultiprocessingPool,
)
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic

logger = logging.getLogger("snuba.consumer.utils")


class TopicNotFound(Exception):
    pass


def get_partition_count(topic: Topic, timeout: float = 2.0) -> int:
    attempts = 0
    while True:
        try:
            logger.info("Attempting to connect to Kafka (attempt %d)...", attempts)
            client = AdminClient(get_default_kafka_configuration(topic=topic))
            cluster_metadata = client.list_topics(timeout=timeout)
            logger.info(f"Checking topic metadata for {topic.value}...")
            topic_metadata = cluster_metadata.topics.get(topic.value)
            break
        except KafkaException as err:
            logger.debug(
                "Connection to Kafka failed (attempt %d)", attempts, exc_info=err
            )
            attempts += 1
            # How many attempts is too many?
            if attempts == 3:
                raise
            time.sleep(1)

    if not topic_metadata:
        raise TopicNotFound(
            f"Topic metadata {topic.value} was not found in topics: {cluster_metadata.topics.keys()}"
        )

    if topic_metadata.error is not None:
        raise KafkaException(topic_metadata.error)

    total_partition_count = len(topic_metadata.partitions.keys())
    logger.info(f"Total of {total_partition_count} partition(s) for {topic.value}.")
    return total_partition_count


@lru_cache(maxsize=None)
def get_reusable_multiprocessing_pool(
    num_processes: int, initializer: Optional[Callable[[], None]]
) -> MultiprocessingPool:
    pool = MultiprocessingPool(num_processes, initializer)

    def shutdown() -> None:
        logger.info("Shutting down multiprocessing pool")
        pool.close()

    atexit.register(shutdown)
    return pool
