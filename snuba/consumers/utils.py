import logging
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic

logger = logging.getLogger("snuba.consumer.utils")


class InvalidTopicName(Exception):
    pass


def get_partition_count(topic: Topic) -> int:
    if not isinstance(topic, Topic):
        raise InvalidTopicName("Must provide a valid Topic.")

    attempts = 0
    while True:
        try:
            logger.info("Attempting to connect to Kafka (attempt %d)...", attempts)
            client = AdminClient(get_default_kafka_configuration(topic=topic))
            cluster_metadata = client.list_topics(timeout=2.0)
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
        raise InvalidTopicName(f"Topic {topic.value} was not found.")

    if topic_metadata.error is not None:
        raise KafkaException(topic_metadata.error)

    total_partition_count = len(topic_metadata.partitions.keys())
    logger.info(f"Total of {total_partition_count} partition(s) for {topic.value}.")
    return total_partition_count
