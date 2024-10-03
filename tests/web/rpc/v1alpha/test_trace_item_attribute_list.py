import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesRequest as TraceItemAttributesRequestProto,
)
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesResponse,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1alpha.trace_item_attribute_list import TraceItemAttributesRequest
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

BASE_TIME = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
) - timedelta(minutes=180)


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
        res["tags"][f"a_tag_{i:03}"] = "blah"  # type: ignore
        res["measurements"][f"b_measurement_{i:03}"] = {"value": 10}  # type: ignore
    return res


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [gen_message(i) for i in range(3)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemAttributes(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemAttributesRequestProto(
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
            ),
            type=AttributeKey.Type.TYPE_STRING,
            limit=10,
            offset=20,
        )
        response = self.app.post(
            "/rpc/TraceItemAttributesRequest/v1alpha",
            data=message.SerializeToString(),
        )
        assert response.status_code == 200

    def test_simple_case_str(self, setup_teardown: Any) -> None:
        message = TraceItemAttributesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int(
                        (
                            datetime(
                                year=BASE_TIME.year,
                                month=BASE_TIME.month,
                                day=BASE_TIME.day,
                                tzinfo=UTC,
                            )
                            - timedelta(days=1)
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
            ),
            limit=10,
            offset=0,
            type=AttributeKey.Type.TYPE_STRING,
        )
        response = TraceItemAttributesRequest().execute(message)
        assert response.tags == [
            TraceItemAttributesResponse.Tag(
                name=f"a_tag_{i:03}", type=AttributeKey.Type.TYPE_STRING
            )
            for i in range(0, 10)
        ]

    def test_simple_case_float(self, setup_teardown: Any) -> None:
        message = TraceItemAttributesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int(
                        (
                            datetime(
                                year=BASE_TIME.year,
                                month=BASE_TIME.month,
                                day=BASE_TIME.day,
                                tzinfo=UTC,
                            )
                            - timedelta(days=1)
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
            ),
            limit=10,
            offset=0,
            type=AttributeKey.Type.TYPE_FLOAT,
        )
        response = TraceItemAttributesRequest().execute(message)
        assert response.tags == [
            TraceItemAttributesResponse.Tag(
                name=f"b_measurement_{i:03}", type=AttributeKey.Type.TYPE_FLOAT
            )
            for i in range(0, 10)
        ]

    def test_with_offset(self, setup_teardown: Any) -> None:
        message = TraceItemAttributesRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(
                    seconds=int(
                        (
                            datetime(
                                year=BASE_TIME.year,
                                month=BASE_TIME.month,
                                day=BASE_TIME.day,
                                tzinfo=UTC,
                            )
                            - timedelta(days=1)
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
            ),
            limit=5,
            offset=10,
            type=AttributeKey.Type.TYPE_FLOAT,
        )
        response = TraceItemAttributesRequest().execute(message)
        assert response.tags == [
            TraceItemAttributesResponse.Tag(
                name="b_measurement_010", type=AttributeKey.Type.TYPE_FLOAT
            ),
            TraceItemAttributesResponse.Tag(
                name="b_measurement_011", type=AttributeKey.Type.TYPE_FLOAT
            ),
            TraceItemAttributesResponse.Tag(
                name="b_measurement_012", type=AttributeKey.Type.TYPE_FLOAT
            ),
            TraceItemAttributesResponse.Tag(
                name="b_measurement_013", type=AttributeKey.Type.TYPE_FLOAT
            ),
            TraceItemAttributesResponse.Tag(
                name="b_measurement_014", type=AttributeKey.Type.TYPE_FLOAT
            ),
        ]
