from typing import Any, Mapping, MutableMapping, Optional

# TODO: Remove type: ignore once next version sentry-kafka-schemas is published
import sentry_kafka_schemas  # type: ignore
from arroyo.processing.strategies.decoder.json import JsonCodec

from snuba.utils.streams.topics import Topic

_HARDCODED_SCHEMAS: Mapping[Topic, Mapping[str, Any]] = {
    # Querylog JSON
    Topic.QUERYLOG: sentry_kafka_schemas.get_schema("querylog")["schema"],
    # Release health metrics
    Topic.METRICS: sentry_kafka_schemas.get_schema("metrics")["schema"],
    # Performance metrics
    Topic.GENERIC_METRICS: sentry_kafka_schemas.get_schema("generic_metrics")["schema"],
    # Error events
    Topic.EVENTS: sentry_kafka_schemas.get_schema("events")["schema"],
}


def get_schema(topic: Topic) -> Optional[Mapping[str, Any]]:
    """
    This is a placeholder. Eventually the schema will be fetched from the
    sentry-kafka-topics library when it gets published.

    This function returns either the schema if it is defined, or None if not.

    """
    return _HARDCODED_SCHEMAS.get(topic)


_cache: MutableMapping[Topic, JsonCodec] = {}


def get_json_codec(topic: Topic) -> JsonCodec:
    if topic not in _cache:
        _cache[topic] = JsonCodec(get_schema(topic))

    return _cache[topic]
