from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Generator, List

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue
from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem as TraceItemMessage

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.trace_item_attribute_values import AttributeValuesRequest
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)
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


@pytest.fixture(autouse=True)
def setup_teardown(eap: None, redis_db: None) -> Generator[List[bytes], None, None]:
    items_storage = get_storage(StorageKey("eap_items"))
    start_timestamp = BASE_TIME
    messages = [
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="herp"),
                "tag2": AnyValue(string_value="herp"),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="herpderp"),
                "tag2": AnyValue(string_value="herp"),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="durp"),
                "tag3": AnyValue(string_value="herp"),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="blah"),
                "tag2": AnyValue(string_value="herp"),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="derpderp"),
                "tag2": AnyValue(string_value="derp"),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={"tag2": AnyValue(string_value="hehe")},
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="some_last_value"),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "sentry.transaction": AnyValue(string_value="*foo"),
            },
        ),
    ]
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore
    yield messages


@pytest.mark.eap
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
        response = self.app.post("/rpc/AttributeValuesRequest/v1", data=message.SerializeToString())
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
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
            value_substring_match="this_definitely_doesnt_exist_93710",
        )
        res = AttributeValuesRequest().execute(req)
        assert res.values == []

    def test_item_id_substring_match(self, setup_teardown: List[bytes]) -> None:
        first_msg_bytes = setup_teardown[0]
        first_msg = TraceItemMessage()
        first_msg.ParseFromString(first_msg_bytes)
        item_id = first_msg.item_id.hex()
        req = TraceItemAttributeValuesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
            ),
            key=AttributeKey(name="sentry.item_id", type=AttributeKey.TYPE_STRING),
            value_substring_match=item_id,
        )
        res = AttributeValuesRequest().execute(req)
        assert res.values == [item_id]

    def test_deprecated_alias_attribute(self) -> None:
        """db.system.name request returns values stored only under deprecated key db.system."""

        items_storage = get_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            [
                gen_item_message(
                    start_timestamp=BASE_TIME,
                    attributes={"db.system": AnyValue(string_value="redis")},
                ),
                gen_item_message(
                    start_timestamp=BASE_TIME,
                    attributes={"db.system": AnyValue(string_value="postgresql")},
                ),
            ],
        )
        message = TraceItemAttributeValuesRequest(
            meta=RequestMeta(
                organization_id=1,
                project_ids=[1],
                cogs_category="something",
                referrer="api.spans.tags-values.rpc",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_NORMAL
                ),
            ),
            key=AttributeKey(name="db.system.name", type=AttributeKey.TYPE_STRING),
            limit=1001,
        )
        response = AttributeValuesRequest().execute(message)
        assert sorted(response.values) == ["postgresql", "redis"]
