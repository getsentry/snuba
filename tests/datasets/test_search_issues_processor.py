import copy
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, MutableMapping, Union

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.datasets.processors.search_issues_processor import (
    InvalidMessageFormat,
    SearchIssueEvent,
    SearchIssuesMessageProcessor,
    ensure_uuid,
)
from snuba.processor import (
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    ReplacementBatch,
)
from snuba.query.snql.parser import parse_snql_query


@pytest.fixture
def message_base() -> SearchIssueEvent:
    return {
        "project_id": 1,
        "organization_id": 2,
        "group_id": 3,
        "event_id": str(uuid.uuid4()),
        "retention_days": 90,
        "primary_hash": str(uuid.uuid4()),
        "datetime": datetime.utcnow().isoformat() + "Z",
        "platform": "other",
        "data": {
            "received": datetime.now().timestamp(),
        },
        "occurrence_data": {
            "id": str(uuid.uuid4()),
            "type": 1,
            "issue_title": "search me",
            "fingerprint": ["one", "two"],
            "detection_time": datetime.now().timestamp(),
        },
    }


class TestSearchIssuesMessageProcessor:
    KAFKA_META = KafkaMessageMetadata(
        offset=0, partition=0, timestamp=datetime(1970, 1, 1)
    )

    processor = SearchIssuesMessageProcessor()
    REQUIRED_COLUMNS = {
        "organization_id",
        "project_id",
        "event_id",
        "search_title",
        "primary_hash",
        "fingerprint",
        "occurrence_id",
        "occurrence_type_id",
        "detection_timestamp",
        "receive_timestamp",
        "client_timestamp",
        "platform",
        "tags.key",
        "tags.value",
    }

    def process_message(
        self, message, version=2, operation="insert", kafka_meta=KAFKA_META
    ):
        return self.processor.process_message((version, operation, message), kafka_meta)

    def assert_required_columns(
        self, processed: Union[InsertBatch, ReplacementBatch, None]
    ):
        assert processed
        assert len(processed.rows) == 1
        assert processed.rows[0].keys() > self.REQUIRED_COLUMNS

    def test_process_message(self, message_base) -> None:
        self.assert_required_columns(self.process_message(message_base))

    def test_fails_unsupported_version(self):
        with pytest.raises(InvalidMessageVersion):
            self.process_message(None, 1, "doesnt_matter")

    def test_fails_invalid_message_type(self):
        with pytest.raises(InvalidMessageType):
            self.process_message(None, 2, "unsupported_operation")

    def test_fails_invalid_occurrence_data(self):
        with pytest.raises(KeyError):
            self.process_message({"data": {"hi": "mom"}})

    def test_fails_unparselable_datetime(self, message_base):
        with pytest.raises(ValueError):
            message_base["datetime"] = datetime.now().isoformat()
            self.process_message(message_base)

    def test_extract_client_timestamp(self, message_base):
        missing_client_timestamp = message_base
        del missing_client_timestamp["datetime"]

        with_data_client_timestamp = copy.deepcopy(missing_client_timestamp)
        with_data_client_timestamp["data"][
            "client_timestamp"
        ] = datetime.now().timestamp()

        with_event_datetime = copy.deepcopy(missing_client_timestamp)
        with_event_datetime["datetime"] = datetime.now().isoformat() + "Z"

        with pytest.raises(InvalidMessageFormat):
            self.process_message(missing_client_timestamp)

        self.process_message(with_data_client_timestamp)
        self.process_message(with_event_datetime)

    def test_extract_user(self, message_base):
        message_with_user = message_base
        message_with_user["data"]["user"] = {
            "id": 1,
            "username": "user",
            "email": "test@example.com",
            "ip_address": "127.0.0.1",
        }
        processed = self.process_message(message_with_user)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert (
            insert_row.items()
            > dict(
                user_name="user",
                user_id="1",
                user_email="test@example.com",
                ip_address_v4="127.0.0.1",
            ).items()
        )

    def test_extract_user_empty(self, message_base):
        message_base["data"]["user"] = {}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert (
            insert_row.items()
            > dict(
                user_name=None,
                user_id=None,
                user_email=None,
            ).items()
        )

    def test_extract_promoted_user_from_tag(self, message_base):
        message_base["data"]["tags"] = {"sentry:user": "user123"}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "user" in insert_row and insert_row["user"] == "user123"

    def test_extract_environment(self, message_base):
        message_base["data"]["environment"] = "prod"
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "environment" in insert_row and insert_row["environment"] == "prod"

    def test_extract_environment_from_tag(self, message_base):
        message_base["data"]["environment"] = "prod"
        message_base["data"]["tags"] = {"environment": "dev"}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "environment" in insert_row and insert_row["environment"] == "dev"

    def test_extract_release(self, message_base):
        message_base["data"]["release"] = "release@123"
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "release" in insert_row and insert_row["release"] == "release@123"

    def test_extract_release_from_tag(self, message_base):
        message_base["data"]["release"] = "release@123"
        message_base["data"]["tags"] = {"sentry:release": "release@456"}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "release" in insert_row and insert_row["release"] == "release@456"

    def test_extract_dist(self, message_base):
        message_base["data"]["dist"] = "dist@123"
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "dist" in insert_row and insert_row["dist"] == "dist@123"

    def test_extract_dist_from_tag(self, message_base):
        message_base["data"]["dist"] = "dist@123"
        message_base["data"]["tags"] = {"sentry:dist": "dist@456"}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "dist" in insert_row and insert_row["dist"] == "dist@456"

    def test_extract_tags(self, message_base):
        message_base["data"]["tags"] = {
            "key": "value",
            "key4": "value4",
            "key3": "value3",
            "key2": "value2",
        }
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "tags.key" in insert_row and "tags.value" in insert_row
        sorted_tags = OrderedDict(sorted(message_base["data"]["tags"].items()))
        assert insert_row["tags.key"] == list(sorted_tags.keys())
        assert insert_row["tags.value"] == list(sorted_tags.values())

    def test_extract_http(self, message_base):
        message_base["data"]["request"] = {
            "method": "GET",
            "headers": [["Referer", "http://example.com"], ["User-Agent", "test"]],
            "extra_stuff": "not_used",
        }
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "http_method" in insert_row and insert_row["http_method"] == "GET"
        assert (
            "http_referer" in insert_row
            and insert_row["http_referer"] == "http://example.com"
        )

    def test_extract_sdk(self, message_base):
        message_base["data"]["sdk"] = {
            "version": "1.2.3",
            "name": "python",
            "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}],
        }
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "sdk_name" in insert_row and insert_row["sdk_name"] == "python"
        assert "sdk_version" in insert_row and insert_row["sdk_version"] == "1.2.3"

    def test_extract_context_filters_non_dict(self, message_base):
        message_base["data"]["contexts"] = {
            "string": "blah",
            "int": 1,
            "float": 1.1,
            "array": ["a", "b", "c"],
            "scalar": {
                "string": "scalar_value",
                "int": 99,
                "float": 123.111,
            },
            "nested_dict": {
                "array": [1, 2, 3],
                "dict": {
                    "key1": "value1",
                    "key2": "value2",
                    "key3": "value3",
                },
                "string": "blah_nested",
                "int": 2,
                "float": 2.2,
            },
        }
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "contexts.key" in insert_row and insert_row["contexts.key"] == [
            "scalar.string",
            "scalar.int",
            "scalar.float",
            "nested_dict.string",
            "nested_dict.int",
            "nested_dict.float",
        ]
        assert "contexts.value" in insert_row and insert_row["contexts.value"] == [
            "scalar_value",
            "99",
            "123.111",
            "blah_nested",
            "2",
            "2.2",
        ]

    def test_extract_context_non_string_dict_keys(self, message_base):
        message_base["data"]["contexts"] = {
            "scalar": {
                1: "val1",
                2: "val2",
                10: 1,
                20: 2,
                100: 1.1,
                200: 2.2,
                1.1: "float_val_1",
                2.2: "float_val_2",
                10.1: 10,
                20.1: 20,
                100.1: 100.1,
                200.1: 200.1,
            },
        }
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "contexts.key" in insert_row and insert_row["contexts.key"] == [
            "scalar.1",
            "scalar.2",
            "scalar.10",
            "scalar.20",
            "scalar.100",
            "scalar.200",
            "scalar.1.1",
            "scalar.2.2",
            "scalar.10.1",
            "scalar.20.1",
            "scalar.100.1",
            "scalar.200.1",
        ]
        assert "contexts.value" in insert_row and insert_row["contexts.value"] == [
            "val1",
            "val2",
            "1",
            "2",
            "1.1",
            "2.2",
            "float_val_1",
            "float_val_2",
            "10",
            "20",
            "100.1",
            "200.1",
        ]

    def test_extract_resource_id(self, message_base):
        resource_id = uuid.uuid4().hex
        message_base["occurrence_data"]["resource_id"] = resource_id
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "resource_id" in insert_row and insert_row["resource_id"] == resource_id

    def test_extract_subtitle(self, message_base):
        sub = "Just according to keikaku. (Translator’s note: Keikaku means plan)"
        message_base["occurrence_data"]["subtitle"] = sub
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "subtitle" in insert_row and insert_row["subtitle"] == sub

    def test_extract_culprit(self, message_base):
        culprit = "it was me, I did it"
        message_base["occurrence_data"]["culprit"] = culprit
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "culprit" in insert_row and insert_row["culprit"] == culprit

    def test_extract_level(self, message_base):
        level = "info"
        message_base["occurrence_data"]["level"] = level
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert "level" in insert_row and insert_row["level"] == level

    def test_extract_trace_id_from_contexts(self, message_base):
        trace_id = str(uuid.uuid4().hex)
        message_base["data"]["contexts"] = {"trace": {"trace_id": trace_id}}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert insert_row["trace_id"] == ensure_uuid(trace_id)

        for invalid_trace_id in ["", "im a little tea pot", 1, 1.1]:
            message_base["data"]["contexts"]["trace"]["trace_id"] = invalid_trace_id
            with pytest.raises(ValueError):
                self.process_message(message_base)

    def test_extract_transaction_duration(self, message_base):
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert insert_row["transaction_duration"] == 0

        now = datetime.utcnow()
        message_base["data"]["start_timestamp"] = int(
            (now - timedelta(seconds=10)).timestamp()
        )
        message_base["data"]["timestamp"] = int(now.timestamp())
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert insert_row["transaction_duration"] == 10 * 1000

        message_base["data"]["start_timestamp"] = "shouldn't be valid"
        message_base["data"]["timestamp"] = {"key": "val"}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert insert_row["transaction_duration"] == 0

    def test_extract_profile_id(self, message_base):
        profile_id = str(uuid.uuid4().hex)
        message_base["data"]["contexts"] = {"profile": {"profile_id": profile_id}}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert insert_row["profile_id"] == ensure_uuid(profile_id)

        for invalid_profile_id in ["", "im a little tea pot", 1, 1.1]:
            message_base["data"]["contexts"]["profile"][
                "profile_id"
            ] = invalid_profile_id
            with pytest.raises(ValueError):
                self.process_message(message_base)

    def test_extract_replay_id(self, message_base):
        replay_id = str(uuid.uuid4().hex)
        message_base["data"]["contexts"] = {"replay": {"replay_id": replay_id}}
        processed = self.process_message(message_base)
        self.assert_required_columns(processed)
        insert_row = processed.rows[0]
        assert insert_row["replay_id"] == ensure_uuid(replay_id)

        for invalid_replay_id in ["", "im a little tea pot", 1, 1.1]:
            message_base["data"]["contexts"]["replay"]["replay_id"] = invalid_replay_id
            with pytest.raises(ValueError):
                self.process_message(message_base)

    def test_ensure_uuid(self):
        with pytest.raises(ValueError):
            ensure_uuid("not_a_uuid")
            ensure_uuid(str(uuid.uuid4().hex))


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
