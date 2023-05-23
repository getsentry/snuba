from typing import Any, Mapping, Optional, Sequence

from arroyo.backends.kafka import build_kafka_configuration
from arroyo.backends.kafka import (
    build_kafka_consumer_configuration as _build_kafka_consumer_configuration,
)

from snuba import settings
from snuba.utils.streams.topics import Topic
from snuba.utils.streams.types import KafkaBrokerConfig


def _get_default_topic_configuration(
    topic: Optional[Topic], slice_id: Optional[int] = None
) -> Mapping[str, Any]:

    if topic is not None:
        if slice_id is not None:
            return settings.SLICED_KAFKA_BROKER_CONFIG.get(
                (topic.value, slice_id), settings.BROKER_CONFIG
            )
        else:
            return settings.KAFKA_BROKER_CONFIG.get(topic.value, settings.BROKER_CONFIG)
    else:
        return settings.BROKER_CONFIG


def get_default_kafka_configuration(
    topic: Optional[Topic] = None,
    slice_id: Optional[int] = None,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    default_topic_config = _get_default_topic_configuration(topic, slice_id)

    return build_kafka_configuration(
        default_topic_config, bootstrap_servers, override_params
    )


def build_kafka_consumer_configuration(
    topic: Optional[Topic],
    group_id: str,
    slice_id: Optional[int] = None,
    auto_offset_reset: Optional[str] = None,
    queued_max_messages_kbytes: Optional[int] = None,
    queued_min_messages: Optional[int] = None,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
    strict_offset_reset: Optional[bool] = None,
) -> KafkaBrokerConfig:
    default_topic_config = _get_default_topic_configuration(topic, slice_id)

    return _build_kafka_consumer_configuration(
        default_topic_config,
        group_id,
        auto_offset_reset,
        queued_max_messages_kbytes,
        queued_min_messages,
        bootstrap_servers,
        override_params,
        strict_offset_reset,
    )


def build_kafka_producer_configuration(
    topic: Optional[Topic],
    slice_id: Optional[int] = None,
    bootstrap_servers: Optional[Sequence[str]] = None,
    override_params: Optional[Mapping[str, Any]] = None,
) -> KafkaBrokerConfig:
    broker_config = get_default_kafka_configuration(
        topic=topic,
        slice_id=slice_id,
        bootstrap_servers=bootstrap_servers,
        override_params=override_params,
    )

    # at time of writing (2022-05-09) lz4 was chosen because it
    # compresses quickly. If more compression is needed at the cost of
    # performance, zstd can be used instead. Recording the query
    # is part of the API request, therefore speed is important
    # perf-testing: https://indico.fnal.gov/event/16264/contributions/36466/attachments/22610/28037/Zstd__LZ4.pdf
    # by default a topic is configured to use whatever compression method the producer used
    # https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#topicconfigs_compression.type
    broker_config["compression.type"] = "lz4"

    return broker_config


def build_default_kafka_producer_configuration() -> KafkaBrokerConfig:
    return build_kafka_producer_configuration(None, None)
