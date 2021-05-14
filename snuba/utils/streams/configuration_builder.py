import logging
from typing import Any, Dict, Mapping, Optional, Sequence

from snuba import settings
from snuba.utils.streams.backends.kafka.configuration import (
    DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    DEFAULT_QUEUED_MIN_MESSAGES,
    build_kafka_configuration_with_overrides,
    build_kafka_consumer_configuration_with_overrides,
)
from snuba.utils.streams.topics import Topic

logger = logging.getLogger(__name__)

KafkaBrokerConfig = Dict[str, Any]


def _get_default_topic_configuration(topic: Optional[Topic]) -> Mapping[str, Any]:
    if topic is not None:
        return settings.KAFKA_BROKER_CONFIG.get(topic.value, settings.BROKER_CONFIG)
    else:
        return settings.BROKER_CONFIG


def get_default_kafka_configuration(
    topic: Optional[Topic] = None,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    default_topic_config = _get_default_topic_configuration(topic)

    return build_kafka_configuration_with_overrides(
        default_topic_config, bootstrap_servers, override_params
    )


def build_kafka_consumer_configuration(
    topic: Optional[Topic],
    group_id: str,
    auto_offset_reset: str = "error",
    queued_max_messages_kbytes: int = DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    queued_min_messages: int = DEFAULT_QUEUED_MIN_MESSAGES,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    default_topic_config = _get_default_topic_configuration(topic)

    return build_kafka_consumer_configuration_with_overrides(
        default_topic_config,
        group_id,
        auto_offset_reset,
        queued_max_messages_kbytes,
        queued_min_messages,
        bootstrap_servers,
        override_params,
    )


def build_kafka_producer_configuration(
    topic: Optional[Topic],
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    broker_config = get_default_kafka_configuration(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        override_params=override_params,
    )
    return broker_config


def build_default_kafka_producer_configuration() -> KafkaBrokerConfig:
    return build_kafka_producer_configuration(None, None)
