from typing import Set

from snuba import settings
from snuba.utils.streams.topics import Topic


class InvalidTopicError(ValueError):
    pass


def validate_storages() -> None:
    validate_topics_with_settings()


def validate_topics_with_settings() -> None:
    """
    This functoion validates topics specified in settings are valid
    topics that have been loaded into the Topics registry (via storage builder).
    """
    topic_names: Set[str] = set([t.value for t in Topic])

    for key in settings.KAFKA_TOPIC_MAP.keys():
        if key not in topic_names:
            raise InvalidTopicError(f"Invalid topic value: {key}")

    for key in settings.KAFKA_BROKER_CONFIG.keys():
        if key not in topic_names:
            raise ValueError(f"Invalid topic value {key}")
