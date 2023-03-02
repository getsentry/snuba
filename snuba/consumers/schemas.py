import json
import os
from typing import Any, Mapping, MutableMapping, Optional

import sentry_kafka_schemas
from arroyo.processing.strategies.decoder.json import JsonCodec

from snuba.utils.streams.topics import Topic


def load_file(filename: str) -> Any:
    with open(os.path.join(os.path.dirname(__file__), "schema_files/", filename)) as f:
        return json.load(f)


_HARDCODED_SCHEMAS: Mapping[Topic, Mapping[str, Any]] = {
    # Querylog JSON
    Topic.QUERYLOG: sentry_kafka_schemas.get_schema("querylog")["schema"],
    # Release health metrics
    Topic.METRICS: load_file("metrics.json"),
    # Performance metrics
    Topic.GENERIC_METRICS: load_file("generic_metrics.json"),
    # XXX(markus): This is copypasted from Relay, need to consolidate at some
    # point
    Topic.EVENTS: load_file("event.json"),
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
