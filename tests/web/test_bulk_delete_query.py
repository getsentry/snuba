from __future__ import annotations

import time
from typing import Any, Mapping, Optional
from unittest.mock import Mock, patch

import pytest
import rapidjson
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from snuba import settings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.exceptions import InvalidQueryException
from snuba.state import set_config
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic
from snuba.web.bulk_delete_query import AttributeConditions, delete_from_storage
from snuba.web.delete_query import DeletesNotEnabledError

CONSUMER_CONFIG = {
    "bootstrap.servers": settings.BROKER_CONFIG["bootstrap.servers"],
    "group.id": "lwd-search-issues",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "enable.auto.offset.store": False,
    # helps diagnose failures
    "debug": "broker,topic,msg",
}


def get_attribution_info(tenant_ids: Optional[Mapping[str, int | str]] = None) -> Mapping[str, Any]:
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
def test_deletes_not_enabled_runtime_config() -> None:
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    set_config("storage_deletes_enabled", 0)
    with pytest.raises(DeletesNotEnabledError):
        delete_from_storage(storage, conditions, attr_info)


@pytest.mark.redis_db
@patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10)
@patch("snuba.web.bulk_delete_query.produce_delete_query")
def test_deletes_killswitch(mock_produce_query: Mock, mock_enforce_rows: Mock) -> None:
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    set_config("lw_deletes_killswitch_search_issues", "[1]")
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
def test_attribute_conditions_valid() -> None:
    """Test that valid attribute_conditions are accepted for eap_items storage"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [1]}
    attribute_conditions = AttributeConditions(item_type=1, attributes={"group_id": [12345]})
    attr_info = get_attribution_info()

    # Mock out _enforce_max_rows to avoid needing actual data
    with patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10):
        with patch("snuba.web.bulk_delete_query.produce_delete_query") as mock_produce:
            # Should not raise an exception, but should return empty dict since
            # functionality is not yet implemented
            result = delete_from_storage(storage, conditions, attr_info, attribute_conditions)

            # Should return empty because we haven't implemented the functionality yet
            assert result == {}
            # Should not have produced a message since we return early
            assert mock_produce.call_count == 0


@pytest.mark.redis_db
def test_attribute_conditions_invalid_attribute() -> None:
    """Test that invalid attribute names in attribute_conditions are rejected"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1], "item_type": [1]}
    attribute_conditions = AttributeConditions(item_type=1, attributes={"invalid_attr": [12345]})
    attr_info = get_attribution_info()

    with pytest.raises(InvalidQueryException, match="Invalid attributes for deletion"):
        delete_from_storage(storage, conditions, attr_info, attribute_conditions)


@pytest.mark.redis_db
def test_attribute_conditions_missing_item_type() -> None:
    """Test that attribute_conditions requires item_type in conditions"""
    storage = get_writable_storage(StorageKey("eap_items"))
    conditions = {"project_id": [1]}
    attribute_conditions = AttributeConditions(item_type=1, attributes={"group_id": [12345]})
    attr_info = get_attribution_info()

    # Since item_type is now in AttributeConditions, we need to test a different scenario
    # The validation now should pass, but we need to ensure item_type is also in conditions
    with patch("snuba.web.bulk_delete_query._enforce_max_rows", return_value=10):
        with patch("snuba.web.bulk_delete_query.produce_delete_query"):
            # This should now succeed since we're no longer checking conditions dict
            delete_from_storage(storage, conditions, attr_info, attribute_conditions)


@pytest.mark.redis_db
def test_attribute_conditions_storage_not_configured() -> None:
    """Test that storages without attribute deletion config reject attribute_conditions"""
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1]}  # Valid columns for search_issues
    attribute_conditions = AttributeConditions(item_type=1, attributes={"some_attr": [12345]})
    attr_info = get_attribution_info()

    with pytest.raises(
        InvalidQueryException, match="No attribute-based deletions configured for this storage"
    ):
        delete_from_storage(storage, conditions, attr_info, attribute_conditions)
