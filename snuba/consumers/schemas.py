import logging
from typing import Any, Mapping, MutableMapping, Optional

import sentry_kafka_schemas
import sentry_sdk
from arroyo.codecs.json import JsonCodec

from snuba.utils.streams.topics import Topic

logger = logging.getLogger(__name__)


def get_schema(topic: Topic) -> Optional[Mapping[str, Any]]:
    """
    This function returns either the schema if it is defined, or None if not.
    """
    try:
        return sentry_kafka_schemas.get_schema(topic.value)["schema"]
    except sentry_kafka_schemas.SchemaNotFound:
        return None
    except Exception as err:
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("snuba_logical_topic", topic.name)
            logger.warning(err, exc_info=True)
        return None


_cache: MutableMapping[Topic, JsonCodec[Any]] = {}


def get_json_codec(topic: Topic) -> JsonCodec[Any]:
    if topic not in _cache:
        _cache[topic] = JsonCodec(schema=get_schema(topic))

    return _cache[topic]
