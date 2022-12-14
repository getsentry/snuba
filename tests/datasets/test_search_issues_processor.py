import uuid
from datetime import datetime
from typing import Any, MutableMapping

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.datasets.processors.search_issues_processor import (
    SearchIssuesMessageProcessor,
    ensure_uuid,
)
from snuba.processor import InvalidMessageType, InvalidMessageVersion
from snuba.query.snql.parser import parse_snql_query


class TestSearchIssuesMessageProcessor:
    KAFKA_META = KafkaMessageMetadata(
        offset=0, partition=0, timestamp=datetime(1970, 1, 1)
    )

    def test_process_message(self) -> None:
        message = (
            2,
            "insert",
            {
                "project_id": 1,
                "organization_id": 2,
                "group_ids": (3,),
                "retention_days": 90,
                "primary_hash": str(uuid.uuid4()),
                "data": {},
                "occurrence_data": {
                    "id": str(uuid.uuid4()),
                    "type": 1,
                    "issue_title": "search me",
                    "fingerprint": ["one", "two"],
                    "detection_time": datetime.utcnow().timestamp(),
                },
            },
        )

        assert SearchIssuesMessageProcessor().process_message(message, self.KAFKA_META)

    def test_fails_unsupported_version(self):
        with pytest.raises(InvalidMessageVersion):
            SearchIssuesMessageProcessor().process_message(
                (1, "doesnt_matter", None), self.KAFKA_META
            )

    def test_fails_invalid_message_type(self):
        with pytest.raises(InvalidMessageType):
            SearchIssuesMessageProcessor().process_message(
                (2, "unsupported_operation", None), self.KAFKA_META
            )

    def test_fails_invalid_occurrence_data(self):
        with pytest.raises(KeyError):
            SearchIssuesMessageProcessor().process_message(
                (2, "insert", {"data": {"hi": "mom"}}), self.KAFKA_META
            )

    def test_ensure_uuid(self):
        with pytest.raises(ValueError):
            bad_uuid = "not_a_uuid"
            message = (
                2,
                "insert",
                {
                    "project_id": 1,
                    "organization_id": 2,
                    "group_ids": (3,),
                    "retention_days": 90,
                    "data": {},
                    "primary_hash": str(uuid.uuid4()),
                    "occurrence_data": {
                        "id": bad_uuid,
                        "type": 1,
                        "issue_title": "search me",
                        "fingerprint": ["one", "two"],
                        "detection_time": datetime.utcnow().timestamp(),
                    },
                },
            )

            assert SearchIssuesMessageProcessor().process_message(
                message, self.KAFKA_META
            )
            ensure_uuid(bad_uuid)


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
        ["timestamp", ">=", "2020-01-01T12:00:00"],
        ["timestamp", "<", "2020-01-02T12:00:00"],
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
