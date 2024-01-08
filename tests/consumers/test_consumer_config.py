import pytest

from snuba.consumers.consumer_config import resolve_consumer_config


def test_consumer_config() -> None:
    resolved = resolve_consumer_config(
        storage_names=["errors"],
        raw_topic="new-events",
        commit_log_topic=None,
        replacements_topic=None,
        slice_id=None,
        bootstrap_servers=["some_server:9092"],
        commit_log_bootstrap_servers=[],
        replacement_bootstrap_servers=["replacements:9092", "replacements-2:9092"],
        max_batch_size=1,
        max_batch_time_ms=1000,
    )

    assert len(resolved.storages) == 1
    assert resolved.storages[0].clickhouse_table_name in ("errors_local", "errors_dist")
    assert resolved.raw_topic.broker_config["bootstrap.servers"] == "some_server:9092"
    assert resolved.raw_topic.physical_topic_name == "new-events"
    assert resolved.raw_topic.logical_topic_name == "events"
    assert resolved.commit_log_topic is not None
    assert resolved.commit_log_topic.physical_topic_name == "snuba-commit-log"
    assert resolved.replacements_topic is not None
    assert resolved.replacements_topic.physical_topic_name == "event-replacements"
    assert (
        resolved.replacements_topic.broker_config["bootstrap.servers"]
        == "replacements:9092,replacements-2:9092"
    )
    assert resolved.dlq_topic is None

    # Invalid storage raises
    with pytest.raises(KeyError):
        resolve_consumer_config(
            storage_names=["invalid_storage"],
            raw_topic=None,
            commit_log_topic=None,
            replacements_topic=None,
            slice_id=None,
            bootstrap_servers=["some_server:9092"],
            commit_log_bootstrap_servers=[],
            replacement_bootstrap_servers=[],
            max_batch_size=1,
            max_batch_time_ms=1000,
        )
