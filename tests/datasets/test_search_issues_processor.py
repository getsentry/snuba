from datetime import datetime
from typing import Any, MutableMapping

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.datasets.processors.search_issues_processor import (
    SearchIssuesMessageProcessor,
)
from snuba.query.snql.parser import parse_snql_query


# TODO: add more tests
class TestSearchIssuesMessageProcessor:
    def test_process_message(self) -> None:
        meta = KafkaMessageMetadata(
            offset=0, partition=0, timestamp=datetime(1970, 1, 1)
        )

        message = (
            1,
            "insert",
            {
                "project_id": 1,
                "organization_id": 2,
                "group_ids": (3,),
                "search_title": "search me",
                "detection_timestamp": datetime.utcnow().timestamp(),
                "retention_days": 90,
                "data": {},
            },
        )

        assert SearchIssuesMessageProcessor().process_message(message, meta)


test_data = [
    ({"conditions": []}, "search_issues"),
]


@pytest.mark.parametrize("query_body, expected_entity", test_data)
def test_data_source(
    query_body: MutableMapping[str, Any],
    expected_entity: EntityKey,
) -> None:
    dataset = get_dataset("search_issues")
    # HACK until these are converted to proper SnQL queries
    if not query_body.get("conditions"):
        query_body["conditions"] = []
    query_body["conditions"] += [
        ["detection_timestamp", ">=", "2020-01-01T12:00:00"],
        ["detection_timestamp", "<", "2020-01-02T12:00:00"],
        ["organization_id", "=", 1],
        ["project_id", "=", 1],
    ]
    if not query_body.get("selected_columns"):
        query_body["selected_columns"] = ["organization_id", "project_id"]
    #
    request = json_to_snql(query_body, "search_issues")
    request.validate()
    query, _ = parse_snql_query(str(request.query), dataset)

    assert query.get_from_clause().key == EntityKey.SEARCH_ISSUES
