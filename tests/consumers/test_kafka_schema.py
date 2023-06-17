import sentry_kafka_schemas

from snuba.datasets.storages.factory import get_writable_storages

TEMPORARILY_SKIPPED_TOPICS = [
    "ingest-sessions",
    "cdc",
    "ingest-replay-events",
    "profiles-call-tree",
    "processed-profiles",
    "generic-events",
]


def test_has_kafka_schema() -> None:
    """
    Source topics for a writable storage must have schema defined.
    Temporarily skipped for a few topics where schemas are in progress.
    """
    for storage in get_writable_storages():
        stream_loader = storage.get_table_writer().get_stream_loader()
        topic_name = stream_loader.get_default_topic_spec().topic.value
        try:
            codec = sentry_kafka_schemas.get_codec(topic_name)
        except sentry_kafka_schemas.SchemaNotFound:
            if topic_name in TEMPORARILY_SKIPPED_TOPICS:
                print("Temporarily skipped validation for topic: %s" % topic_name)
            else:
                raise

        assert codec is not None
