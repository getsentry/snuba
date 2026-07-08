from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any
from unittest.mock import Mock, patch

import pytest
import rapidjson
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba import settings
from snuba.clusters.cluster import UndefinedClickhouseCluster
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.types import AttributeConditions
from snuba.query.exceptions import InvalidQueryException
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic
from snuba.web.bulk_delete_query import delete_from_storage
from snuba.web.delete_query import DeletesNotEnabledError

# TraceItemType values from sentry_protos
TRACE_ITEM_TYPE_SPAN = 1
TRACE_ITEM_TYPE_OCCURRENCE = 7

CONSUMER_CONFIG = {
    "bootstrap.servers": settings.BROKER_CONFIG["bootstrap.servers"],
    "group.id": "lwd-search-issues",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "enable.auto.offset.store": False,
    # helps diagnose failures
    "debug": "broker,topic,msg",
}


def get_attribution_info(tenant_ids: Mapping[str, int | str] | None = None) -> Mapping[str, Any]:
    return {
        "tenant_ids": tenant_ids or {"project_id": 1, "organization_id": 1},
        "referrer": "some_referrer",
        "app_id": "test",
        "team": "test",
        "parent_api": "test",
        "feature": "test",
    }


@patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10)
def test_delete_success(mock_enforce_max_row: Mock) -> None:
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [Topic.LW_DELETIONS_GENERIC_EVENTS])

    consumer = Consumer(CONSUMER_CONFIG)
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    # just give in second before subscribing
    time.sleep(2.0)
    consumer.subscribe([Topic.LW_DELETIONS_GENERIC_EVENTS.value])

    result = delete_from_storage(storage, conditions, attr_info)
    assert result["search_issues_local_v2"]["data"] == [{"rows_to_delete": 10}]

    attempts = 12
    kafka_msg = None
    while attempts > 0 and not kafka_msg:
        kafka_msg = consumer.poll(1.0)
        attempts -= 1

    # assumes that we didn't get the message because the
    # partition wasn't assigned quickly enough
    assert kafka_msg, "No message after 11 poll attempts"

    message = rapidjson.loads(kafka_msg.value())
    assert message["rows_to_delete"] == 10
    assert message == {
        "rows_to_delete": 10,
        "storage_name": "search_issues",
        "conditions": conditions,
        "tenant_ids": {"project_id": 1, "organization_id": 1},
    }
    consumer.close()


def test_deletes_not_enabled_on_storage() -> None:
    storage = get_writable_storage(StorageKey("replays"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    with pytest.raises(DeletesNotEnabledError):
        delete_from_storage(storage, conditions, attr_info)


@pytest.mark.redis_db
@override_options("snuba", {"storage_deletes_enabled": False})
def test_deletes_not_enabled_runtime_config() -> None:
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    with pytest.raises(DeletesNotEnabledError):
        delete_from_storage(storage, conditions, attr_info)


@pytest.mark.redis_db
@patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10)
@patch("snuba.web.bulk_delete_query.produce_delete_query")
@override_options("snuba", {"lw_deletes_killswitch": {"search_issues": "[1]"}})
def test_deletes_killswitch(mock_produce_query: Mock, mock_enforce_rows: Mock) -> None:
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    delete_from_storage(storage, conditions, attr_info)
    mock_produce_query.assert_not_called()


@pytest.mark.redis_db
def test_delete_invalid_column_type() -> None:
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions: dict[str, list[int | str]] = {
        "project_id": ["invalid_project"],
        "group_id": [1, 2, 3, 4],
    }
    attr_info = get_attribution_info()

    with pytest.raises(InvalidQueryException):
        delete_from_storage(storage, conditions, attr_info)


@pytest.mark.redis_db
def test_delete_invalid_column_name() -> None:
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "bad_column": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    with pytest.raises(InvalidQueryException):
        delete_from_storage(storage, conditions, attr_info)


@pytest.mark.redis_db
def test_attribute_conditions_invalid_item_type() -> None:
    """Test that attribute_conditions with wrong item_type (span instead of occurrence) are rejected"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [TRACE_ITEM_TYPE_SPAN]}
    # Using span (1) but config only allows occurrence (7)
    attribute_conditions = AttributeConditions(
        item_type=TRACE_ITEM_TYPE_SPAN,
        attributes={
            "group_id": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="group_id"), [12345])
        },
    )
    attr_info = get_attribution_info()

    with pytest.raises(
        InvalidQueryException,
        match="No attribute-based deletions configured for item_type span",
    ):
        delete_from_storage(storage, conditions, attr_info, attribute_conditions)


@pytest.mark.redis_db
def test_attribute_conditions_valid_occurrence() -> None:
    """Test that valid attribute_conditions are accepted for occurrence item_type"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [TRACE_ITEM_TYPE_OCCURRENCE]}
    attribute_conditions = AttributeConditions(
        item_type=TRACE_ITEM_TYPE_OCCURRENCE,
        attributes={
            "group_id": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="group_id"), [12345])
        },
    )
    attr_info = get_attribution_info()

    # Mock out _enforce_max_rows to avoid needing actual data
    with (
        patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10),
        patch("snuba.web.bulk_delete_query.produce_delete_query") as mock_produce,
    ):
        # Should not raise an exception, but should return empty dict since
        # functionality is not yet launched (permit_delete_by_attribute=0 by default)
        result = delete_from_storage(storage, conditions, attr_info, attribute_conditions)

        # Should return empty because the feature flag is off
        assert result == {}
        # Should not have produced a message since we return early
        assert mock_produce.call_count == 0


@pytest.mark.redis_db
def test_attribute_conditions_invalid_attribute() -> None:
    """Test that invalid attribute names in attribute_conditions are rejected"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [TRACE_ITEM_TYPE_OCCURRENCE]}
    # Using valid item_type (occurrence/7) but invalid attribute
    attribute_conditions = AttributeConditions(
        item_type=TRACE_ITEM_TYPE_OCCURRENCE,
        attributes={
            "invalid_attr": (
                AttributeKey(type=AttributeKey.Type.TYPE_INT, name="invalid_attr"),
                [12345],
            )
        },
    )
    attr_info = get_attribution_info()

    with pytest.raises(InvalidQueryException, match="Invalid attributes for deletion"):
        delete_from_storage(storage, conditions, attr_info, attribute_conditions)


@pytest.mark.redis_db
def test_attribute_conditions_missing_item_type() -> None:
    """Test that attribute_conditions requires item_type in conditions"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1]}
    attribute_conditions = AttributeConditions(
        item_type=TRACE_ITEM_TYPE_OCCURRENCE,
        attributes={
            "group_id": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="group_id"), [12345])
        },
    )
    attr_info = get_attribution_info()

    # Since item_type is now in AttributeConditions, we need to test a different scenario
    # The validation now should pass, but we need to ensure item_type is also in conditions
    with (
        patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10),
        patch("snuba.web.bulk_delete_query.produce_delete_query"),
    ):
        # This should now succeed since we're no longer checking conditions dict
        delete_from_storage(storage, conditions, attr_info, attribute_conditions)


@pytest.mark.redis_db
def test_attribute_conditions_storage_not_configured() -> None:
    """Test that storages without attribute deletion config reject attribute_conditions"""
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1]}  # Valid columns for search_issues
    attribute_conditions = AttributeConditions(
        item_type=1,
        attributes={
            "some_attr": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="some_attr"), [12345])
        },
    )
    attr_info = get_attribution_info()

    with pytest.raises(
        InvalidQueryException, match="No attribute-based deletions configured for this storage"
    ):
        delete_from_storage(storage, conditions, attr_info, attribute_conditions)


@pytest.mark.redis_db
def test_attribute_conditions_feature_flag_enabled() -> None:
    """Test that attribute_conditions are processed when feature flag is enabled"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [TRACE_ITEM_TYPE_OCCURRENCE]}
    attribute_conditions = AttributeConditions(
        item_type=TRACE_ITEM_TYPE_OCCURRENCE,
        attributes={
            "group_id": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="group_id"), [12345])
        },
    )
    attr_info = get_attribution_info()

    # Mock out _enforce_max_rows to avoid needing actual data
    with (
        override_options("snuba", {"permit_delete_by_attribute": True}),
        patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10),
        patch("snuba.web.bulk_delete_query.produce_delete_query") as mock_produce,
    ):
        # Should process normally and produce a message
        result = delete_from_storage(storage, conditions, attr_info, attribute_conditions)

        # Should have produced a message
        assert mock_produce.call_count == 1
        # Should return success results
        assert result != {}

        # Verify the message includes attribute_conditions
        call_args = mock_produce.call_args[0][0]
        assert "attribute_conditions" in call_args
        assert call_args["attribute_conditions"] == {
            "group_id": {
                "attr_key_name": "group_id",
                "attr_key_type": AttributeKey.TYPE_INT,
                "attr_values": [12345],
            }
        }
        assert call_args["attribute_conditions_item_type"] == TRACE_ITEM_TYPE_OCCURRENCE


@pytest.mark.redis_db
def test_eap_items_counts_each_table_against_its_readonly_replica() -> None:
    """
    The downsampled EAP tables hold different row counts than the read/write
    table, so each must be counted individually (rather than re-running the same
    count against eap_items_1_dist N times, which was an N+1). Counting is a
    read-only query, so each count targets the table's read-only `*_ro` replica
    storage, keeping it off the read/write cluster.
    """
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [TRACE_ITEM_TYPE_OCCURRENCE]}
    attribute_conditions = AttributeConditions(
        item_type=TRACE_ITEM_TYPE_OCCURRENCE,
        attributes={
            "group_id": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="group_id"), [12345])
        },
    )
    attr_info = get_attribution_info()

    with override_options("snuba", {"permit_delete_by_attribute": True}):
        with (
            patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10) as mock_enforce,
            patch("snuba.web.bulk_delete_query.produce_delete_query"),
        ):
            delete_from_storage(storage, conditions, attr_info, attribute_conditions)

        count_storage_keys = [
            call.kwargs["count_storage_key"] for call in mock_enforce.call_args_list
        ]
        # One count per table, each against a distinct read-only replica storage
        # (not the same table N times, which was the N+1).
        assert len(count_storage_keys) == 4
        assert len(set(count_storage_keys)) == 4
        # Every count runs against a read-only replica storage, never the
        # read/write storage that the delete itself targets.
        assert {key.value for key in count_storage_keys} == {
            "eap_items_ro",
            "eap_items_downsample_8_ro",
            "eap_items_downsample_64_ro",
            "eap_items_downsample_512_ro",
        }


def test_count_storage_key_mapping_without_readonly_storage_set() -> None:
    """A storage without a read-only storage set (search_issues) has no replica
    to count against, so the mapping is empty and the caller falls back to the
    read/write table."""
    from snuba.web.bulk_delete_query import _count_storage_key_by_local_table

    storage = get_writable_storage(StorageKey("search_issues"))
    assert _count_storage_key_by_local_table(storage) == {}


def test_count_storage_key_mapping_without_readonly_cluster() -> None:
    """When the read-only cluster is not configured in this environment, the
    mapping is empty so counts fall back to the read/write table rather than
    failing against a missing cluster."""
    from snuba.web.bulk_delete_query import _count_storage_key_by_local_table

    # UndefinedClickhouseCluster is imported at module scope (before pytest
    # collection) so its class identity matches bulk_delete_query's `except`
    # binding even when another test reloads snuba.clusters.cluster.
    storage = get_writable_storage(StorageKey("eap_items"))
    with patch(
        "snuba.web.bulk_delete_query.get_cluster",
        side_effect=UndefinedClickhouseCluster("not configured"),
    ):
        assert _count_storage_key_by_local_table(storage) == {}
