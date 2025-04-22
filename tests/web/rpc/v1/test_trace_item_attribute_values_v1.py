import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.trace_item_attribute_values import AttributeValuesRequest
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

BASE_TIME = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
) - timedelta(minutes=180)
COMMON_META = RequestMeta(
    project_ids=[1, 2, 3],
    organization_id=1,
    cogs_category="something",
    referrer="something",
    start_timestamp=Timestamp(
        seconds=int(
            datetime(
                year=BASE_TIME.year,
                month=BASE_TIME.month,
                day=BASE_TIME.day,
                tzinfo=UTC,
            ).timestamp()
        )
    ),
    end_timestamp=Timestamp(
        seconds=int(
            (
                datetime(
                    year=BASE_TIME.year,
                    month=BASE_TIME.month,
                    day=BASE_TIME.day,
                    tzinfo=UTC,
                )
                + timedelta(days=1)
            ).timestamp()
        )
    ),
)


def gen_message(tags: Mapping[str, str]) -> Mapping[str, Any]:
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {},
        "measurements": {},
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {},
        "span_id": uuid.uuid4().hex,
        "tags": tags,
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(BASE_TIME.timestamp() * 1000),
        "start_timestamp_precise": BASE_TIME.timestamp(),
        "end_timestamp_precise": BASE_TIME.timestamp() + 1,
    }


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    items_storage = get_storage(StorageKey("eap_items"))
    messages = [
        gen_message({"tag1": "herp", "tag2": "herp"}),
        gen_message({"tag1": "herpderp", "tag2": "herp"}),
        gen_message({"tag1": "durp", "tag3": "herp"}),
        gen_message({"tag1": "blah", "tag2": "herp"}),
        gen_message({"tag1": "derpderp", "tag2": "derp"}),
        gen_message({"tag2": "hehe"}),
        gen_message({"tag1": "some_last_value"}),
        gen_message({"sentry.transaction": "*foo"}),
    ]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemAttributes(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
            limit=10,
        )
        response = self.app.post(
            "/rpc/AttributeValuesRequest/v1", data=message.SerializeToString()
        )
        assert response.status_code == 200

    def test_simple_case(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["blah", "derpderp", "durp", "herp", "herpderp"]

    def test_with_value_substring_match(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
            value_substring_match="erp",
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["derpderp", "herp", "herpderp"]

    def test_empty_results(self) -> None:
        req = TraceItemAttributeValuesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int((BASE_TIME - timedelta(days=1)).timestamp())
                ),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(days=1)).timestamp())
                ),
            ),
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
            value_substring_match="this_definitely_doesnt_exist_93710",
        )
        res = AttributeValuesRequest().execute(req)
        assert res.values == []

    def test_transaction(self) -> None:
        req = TraceItemAttributeValuesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int((BASE_TIME - timedelta(days=1)).timestamp())
                ),
                end_timestamp=Timestamp(
                    seconds=int((BASE_TIME + timedelta(days=1)).timestamp())
                ),
            ),
            key=AttributeKey(name="sentry.segment_name", type=AttributeKey.TYPE_STRING),
            value_substring_match="",
        )
        res = AttributeValuesRequest().execute(req)
        assert res.values == ["*foo"]
