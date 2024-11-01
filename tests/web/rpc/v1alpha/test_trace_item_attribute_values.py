import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    AttributeValuesRequest as AttributeValuesRequestProto,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1alpha.trace_item_attribute_values import AttributeValuesRequest
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
    messages = [
        gen_message({"tag1": "herp", "tag2": "herp"}),
        gen_message({"tag1": "herpderp", "tag2": "herp"}),
        gen_message({"tag1": "durp", "tag3": "herp"}),
        gen_message({"tag1": "blah", "tag2": "herp"}),
        gen_message({"tag1": "derpderp", "tag2": "derp"}),
        gen_message({"tag2": "hehe"}),
        gen_message({"tag1": "some_last_value"}),
    ]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemAttributes(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = AttributeValuesRequestProto(
            meta=COMMON_META,
            name="tag1",
            limit=10,
            offset=20,
        )
        response = self.app.post(
            "/rpc/AttributeValuesRequest/v1alpha", data=message.SerializeToString()
        )
        assert response.status_code == 200

    def test_simple_case(self, setup_teardown: Any) -> None:
        message = AttributeValuesRequestProto(meta=COMMON_META, limit=5, name="tag1")
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["blah", "derpderp", "durp", "herp", "herpderp"]

    def test_offset(self, setup_teardown: Any) -> None:
        message = AttributeValuesRequestProto(
            meta=COMMON_META, limit=5, offset=1, name="tag1"
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == [
            "derpderp",
            "durp",
            "herp",
            "herpderp",
            "some_last_value",
        ]

    def test_with_value_filter(self, setup_teardown: Any) -> None:
        message = AttributeValuesRequestProto(
            meta=COMMON_META, limit=5, name="tag1", value_substring_match="erp"
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["derpderp", "herp", "herpderp"]
