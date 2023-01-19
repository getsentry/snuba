from typing import Any, Mapping, MutableMapping, Optional

from arroyo.processing.strategies.decoder import JsonCodec

from snuba.utils.streams.topics import Topic


def get_schema(topic: Topic) -> Optional[Mapping[str, Any]]:
    """
    This is a placeholder. Eventually the schema will be fetched from the
    sentry-kafka-topics library when it gets published.

    This function returns either the schema if it is defined, or None if not.

    """
    return None


_cache: MutableMapping[Topic, JsonCodec] = {}


def get_json_codec(topic: Topic) -> JsonCodec:
    if topic not in _cache:
        _cache[topic] = JsonCodec(get_schema(topic))

    return _cache[topic]
