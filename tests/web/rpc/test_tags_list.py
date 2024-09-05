import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TagsListRequest,
    TagsListResponse,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.tags_list import tags_list_query
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


def gen_message(id: int) -> Mapping[str, Any]:
    res = {
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
        "sentry_tags": {
            "category": "http",
        },
        "span_id": uuid.uuid4().hex,
        "tags": {
            "http.status_code": "200",
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(BASE_TIME.timestamp() * 1000),
        "start_timestamp_precise": BASE_TIME.timestamp(),
        "end_timestamp_precise": BASE_TIME.timestamp() + 1,
    }
    for i in range(id * 10, id * 10 + 10):
        res["tags"][f"a_tag_{i:03}"] = "blah"
        res["measurements"][f"b_measurement_{i:03}"] = {"value": 10}
    return res


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [gen_message(i) for i in range(3)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTagsList(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TagsListRequest(
            meta=RequestMeta(
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
                        datetime(
                            year=BASE_TIME.year,
                            month=BASE_TIME.month,
                            day=BASE_TIME.day + 1,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
            ),
            limit=10,
            offset=20,
        )
        response = self.app.post(
            "/rpc/TagsListRequest", data=message.SerializeToString()
        )
        assert response.status_code == 200

    def test_simple_case(self, setup_teardown: Any) -> None:
        message = TagsListRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int(
                        datetime(
                            year=BASE_TIME.year,
                            month=BASE_TIME.month,
                            day=BASE_TIME.day - 1,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
                end_timestamp=Timestamp(
                    seconds=int(
                        datetime(
                            year=BASE_TIME.year,
                            month=BASE_TIME.month,
                            day=BASE_TIME.day + 1,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
            ),
            limit=10,
            offset=0,
        )
        response = tags_list_query(message)
        assert response.tags == [
            TagsListResponse.Tag(
                name=f"a_tag_{i:03}", type=TagsListResponse.TYPE_STRING
            )
            for i in range(0, 10)
        ]

    def test_with_offset(self, setup_teardown: Any) -> None:
        message = TagsListRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int(
                        datetime(
                            year=BASE_TIME.year,
                            month=BASE_TIME.month,
                            day=BASE_TIME.day - 1,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
                end_timestamp=Timestamp(
                    seconds=int(
                        datetime(
                            year=BASE_TIME.year,
                            month=BASE_TIME.month,
                            day=BASE_TIME.day + 1,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
            ),
            limit=5,
            offset=29,
        )
        response = tags_list_query(message)
        assert response.tags == [
            TagsListResponse.Tag(name="a_tag_029", type=TagsListResponse.TYPE_STRING),
            TagsListResponse.Tag(
                name="b_measurement_000", type=TagsListResponse.TYPE_NUMBER
            ),
            TagsListResponse.Tag(
                name="b_measurement_001", type=TagsListResponse.TYPE_NUMBER
            ),
            TagsListResponse.Tag(
                name="b_measurement_002", type=TagsListResponse.TYPE_NUMBER
            ),
            TagsListResponse.Tag(
                name="b_measurement_003", type=TagsListResponse.TYPE_NUMBER
            ),
        ]
