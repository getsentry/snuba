from __future__ import annotations

from typing import Any, Mapping, Optional
from unittest.mock import Mock, patch

import pytest
import rapidjson
from confluent_kafka import Consumer

from snuba import settings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.exceptions import InvalidQueryException
from snuba.state import set_config
from snuba.utils.streams.topics import Topic
from snuba.web.bulk_delete_query import _get_kafka_producer, delete_from_storage
from snuba.web.delete_query import DeletesNotEnabledError

CONSUMER_CONFIG = {
    "bootstrap.servers": settings.BROKER_CONFIG["bootstrap.servers"],
    "group.id": "lwd-search-issues",
    "enable.auto.commit": True,
    "auto.offset.reset": "latest",
}


def get_attribution_info(
    tenant_ids: Optional[Mapping[str, int | str]] = None
) -> Mapping[str, Any]:
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
    consumer = Consumer(CONSUMER_CONFIG)
    storage = get_writable_storage(StorageKey("search_issues"))
    conditions = {"project_id": [1], "group_id": [1, 2, 3, 4]}
    attr_info = get_attribution_info()

    consumer.subscribe([Topic.LW_DELETIONS.value])

    result = delete_from_storage(storage, conditions, attr_info)
    assert result["search_issues_local_v2"]["data"] == [{"rows_to_delete": 10}]

    # make sure message got delivered
    p = _get_kafka_producer()
    p.flush()

    kafka_msg = consumer.poll(10.0)
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
