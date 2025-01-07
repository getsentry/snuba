import sentry_kafka_schemas

from snuba.utils.streams.topics import Topic


def test_valid_topics() -> None:
    # Ensures that Snuba's topic list matches those registered in sentry-kafka-schemas
    for topic in Topic:
        try:
            sentry_kafka_schemas.get_topic(
                topic.value
            )  # Throws an exception if topic not defined
        except sentry_kafka_schemas.SchemaNotFound:
            # These topics are not in use but have not yet been removed from snuba's codebase
            deprecated_topics = (Topic.CDC,)

            if topic not in deprecated_topics:
                raise
