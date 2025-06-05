import logging
from typing import Sequence

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.utils.streams.topics import Topic

logger = logging.getLogger(__name__)


def create_topics(client: AdminClient, topics: Sequence[Topic]) -> None:
    topics_to_create = {}

    for topic in topics:
        topic_spec = KafkaTopicSpec(topic)
        logger.debug("Adding topic %s to creation list", topic_spec.topic_name)
        topics_to_create[topic_spec.topic_name] = NewTopic(
            topic_spec.topic_name,
            num_partitions=topic_spec.partitions_number,
            replication_factor=topic_spec.replication_factor,
            config=topic_spec.topic_creation_config,
        )

    logger.info("Creating Kafka topics...")
    for topic, future in client.create_topics(
        list(topics_to_create.values()), operation_timeout=1
    ).items():
        try:
            future.result()
            logger.info("Topic %s created", topic)
        except KafkaException as err:
            if err.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                logger.error("Failed to create topic %s", topic, exc_info=err)
