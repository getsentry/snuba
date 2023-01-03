from typing import Any, Mapping, Optional

from snuba.utils.streams.topics import Topic


def get_schema(topic: Topic) -> Optional[Mapping[str, Any]]:
    """
    This is a placeholder. Eventually the schema will be fetched from the
    sentry-kafka-topics library when it gets published.

    This function returns either the schema if it is defined, or None if not.

    """
    return None
