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


def get_topic_name(topic: Topic) -> str:
    return settings.KAFKA_TOPIC_MAP.get(topic.value, topic.value)


def get_topic_creation_config(topic: Topic) -> Mapping[str, str]:
    config = {Topic.EVENTS: {"message.timestamp.type": "LogAppendTime"}}
    return config.get(topic, {})
