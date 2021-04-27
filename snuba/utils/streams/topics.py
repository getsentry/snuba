from enum import Enum
from typing import Mapping
from snuba import settings

# These are the default topic names, they can be changed via settings
class Topic(Enum):
    EVENTS = "events"
    EVENT_REPLACEMENTS = "event-replacements"
    COMMIT_LOG = "snuba-commit-log"
    CDC = "cdc"
    OUTCOMES = "outcomes"
    SESSIONS = "ingest-sessions"
    QUERYLOG = "snuba-queries"


class KafkaTopicSpec:
    def __init__(self, topic: Topic) -> None:
        self.__topic = topic

    @property
    def topic(self) -> Topic:
        return self.__topic

    @property
    def topic_name(self) -> str:
        return get_topic_name(self.__topic)

    @property
    def partitions_number(self) -> int:
        # TODO: This references the actual topic name for backward compatibility.
        # It should be changed to the logical name for consistency with KAFKA_TOPIC_MAP
        # and KAFKA_BROKER_CONFIG
        return settings.TOPIC_PARTITION_COUNTS.get(self.topic_name, 1)

    @property
    def replication_factor(self) -> int:
        return 1

    @property
    def topic_creation_config(self) -> Mapping[str, str]:
        return get_topic_creation_config(self.__topic)


def get_topic_name(topic: Topic) -> str:
    return settings.KAFKA_TOPIC_MAP.get(topic.value, topic.value)


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    config = {Topic.EVENTS: {"message.timestamp.type": "LogAppendTime"}}
    return config.get(topic, {})
