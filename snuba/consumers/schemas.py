import logging
from typing import Any, MutableMapping, Optional

import sentry_kafka_schemas
import sentry_sdk
from sentry_kafka_schemas.codecs import Codec
from sentry_kafka_schemas.codecs.json import JsonCodec

from snuba.utils.streams.topics import Topic

logger = logging.getLogger(__name__)


def _get_codec_impl(topic: Topic) -> Optional[Codec[Any]]:
    """
    This function returns either the schema if it is defined, or None if not.
    """
    try:
        return sentry_kafka_schemas.get_codec(topic.value)
    except sentry_kafka_schemas.SchemaNotFound:
        return None
    except Exception:
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("snuba_logical_topic", topic.name)
            logger.warning(f"Validation error - {topic.name}", exc_info=True)
        return None


_NOOP_CODEC: Codec[Any] = JsonCodec(json_schema=None)
_cache: MutableMapping[Topic, Codec[Any]] = {}


def get_json_codec(topic: Topic) -> Codec[Any]:
    if topic not in _cache:
        _cache[topic] = _get_codec_impl(topic) or _NOOP_CODEC

    return _cache[topic]
