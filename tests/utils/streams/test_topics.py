import sentry_kafka_schemas

from snuba.utils.streams.topics import Topic


def test_valid_topics() -> None:
    # Ensures that Snuba's topic list matches those registered in sentry-kafka-schemas
    for topic in Topic:
        try:
            sentry_kafka_schemas.get_topic(topic.value)  # Throws an exception if topic not defined
        except sentry_kafka_schemas.SchemaNotFound:
            # These topics are not in use but have not yet been removed from snuba's codebase
            deprecated_topics = (
                Topic.CDC,
                Topic.EAP_SPANS_COMMIT_LOG,
                Topic.SUBSCRIPTION_SCHEDULED_EAP_SPANS,
                Topic.SUBSCRIPTION_RESULTS_EAP_SPANS,
                Topic.EAP_MUTATIONS,
                Topic.OURLOGS,
            )
            # New topics pending registration in sentry-kafka-schemas.
            # Remove the entry from this tuple once a schema has been added.
            pending_schema_registration = (Topic.EAP_ITEMS_LATE_ARRIVALS,)

            if topic not in deprecated_topics and topic not in pending_schema_registration:
                raise
