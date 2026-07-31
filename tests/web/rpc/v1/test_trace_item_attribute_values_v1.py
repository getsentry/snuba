from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any

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

from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.trace_item_attribute_values import AttributeValuesRequest
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(minutes=180)
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
def setup_teardown(eap: None, redis_db: None) -> Generator[list[bytes]]:
    items_storage = get_writable_storage(StorageKey("eap_items"))
    start_timestamp = BASE_TIME
    messages = [
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="herp"),
                "tag2": AnyValue(string_value="herp"),
                "custom_flag": AnyValue(bool_value=True),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "tag1": AnyValue(string_value="herpderp"),
                "tag2": AnyValue(string_value="herp"),
                "custom_flag": AnyValue(bool_value=False),
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
                "sentry.is_segment": AnyValue(bool_value=False),
            },
        ),
        gen_item_message(
            start_timestamp=start_timestamp,
            attributes={
                "sentry.transaction": AnyValue(string_value="*foo"),
            },
        ),
    ]
    write_raw_unprocessed_events(items_storage, messages)
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
        assert response.values == ["derpderp", "blah", "durp", "herp", "herpderp"]
        assert response.counts == [2, 1, 1, 1, 1]

    def _write_version_values(self) -> None:
        # Distinct version-like values (one occurrence each). Lexicographically
        # "1.2.10" < "1.2.2" < "1.2.9"; naturally "1.2.2" < "1.2.9" < "1.2.10".
        # Written per-test (not in the shared fixture) so the extra items don't
        # perturb the count-based assertions in other tests.
        items_storage = get_writable_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(
            items_storage,
            [
                gen_item_message(
                    start_timestamp=BASE_TIME,
                    attributes={"natural_ver": AnyValue(string_value=v)},
                )
                for v in ("1.2.9", "1.2.10", "1.2.2")
            ],
        )

    def test_semver_sort(self, setup_teardown: Any) -> None:
        # SORT_SEMVER applies the semver key, so version components sort
        # numerically (1.2.2 < 1.2.9 < 1.2.10) regardless of the attribute.
        self._write_version_values()
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=10,
            key=AttributeKey(name="natural_ver", type=AttributeKey.TYPE_STRING),
            order_by=TraceItemAttributeValuesRequest.OrderBy(
                column=TraceItemAttributeValuesRequest.OrderBy.COLUMN_VALUE,
                sort=TraceItemAttributeValuesRequest.OrderBy.SORT_SEMVER,
            ),
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["1.2.2", "1.2.9", "1.2.10"]

    def test_default_sort_is_lexicographic(self, setup_teardown: Any) -> None:
        # Unset sort keeps the historical lexicographic ordering, where "1.2.10"
        # sorts before "1.2.2" and "1.2.9".
        self._write_version_values()
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=10,
            key=AttributeKey(name="natural_ver", type=AttributeKey.TYPE_STRING),
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["1.2.10", "1.2.2", "1.2.9"]

    def test_release_sort_is_semver_aware(self, setup_teardown: Any) -> None:
        # SORT_SEMVER is the semver sort, so prerelease sorts before its stable
        # release and the "pkg@" prefix is stripped.
        items_storage = get_writable_storage(StorageKey("eap_items"))
        releases = ["1.2.3-beta.1", "1.2.3", "1.2.9", "1.2.10", "my-pkg@2.0.0"]
        write_raw_unprocessed_events(
            items_storage,
            [
                gen_item_message(
                    start_timestamp=BASE_TIME,
                    attributes={"sentry.release": AnyValue(string_value=r)},
                )
                for r in releases
            ],
        )
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=10,
            key=AttributeKey(name="sentry.release", type=AttributeKey.TYPE_STRING),
            order_by=TraceItemAttributeValuesRequest.OrderBy(
                column=TraceItemAttributeValuesRequest.OrderBy.COLUMN_VALUE,
                sort=TraceItemAttributeValuesRequest.OrderBy.SORT_SEMVER,
            ),
        )
        response = AttributeValuesRequest().execute(message)
        # gen_item_message stamps a default sentry.release on every fixture item,
        # so other release values may appear; assert only the relative order of
        # the releases we wrote, which must follow semver ordering.
        ordered = [v for v in response.values if v in set(releases)]
        assert ordered == [
            "1.2.3-beta.1",
            "1.2.3",
            "1.2.9",
            "1.2.10",
            "my-pkg@2.0.0",
        ]

    def test_with_value_substring_match(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
            value_substring_match="erp",
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["derpderp", "herp", "herpderp"]
        assert response.counts == [2, 1, 1]

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
        assert res.counts == []

    def test_item_id_substring_match(self, setup_teardown: list[bytes]) -> None:
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
        assert res.counts == [1]

    def test_deprecated_alias_attribute(self) -> None:
        """db.system.name request returns values stored only under deprecated key db.system."""

        items_storage = get_writable_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(
            items_storage,
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
        assert response.counts == [1, 1]

    def test_pagination(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=1,
            key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["derpderp"]
        assert response.counts == [2]

        for expected in ["blah", "durp", "herp", "herpderp"]:
            message = TraceItemAttributeValuesRequest(
                meta=COMMON_META,
                limit=1,
                key=AttributeKey(name="tag1", type=AttributeKey.TYPE_STRING),
                page_token=response.page_token,
            )
            response = AttributeValuesRequest().execute(message)
            assert response.values == [expected]
            assert response.counts == [1]

    def test_boolean_case(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="sentry.is_segment", type=AttributeKey.TYPE_BOOLEAN),
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["true", "false"]
        assert response.counts == [8, 1]

    def test_boolean_case_with_semver_sort_does_not_crash(self, setup_teardown: Any) -> None:
        # SORT_SEMVER only applies the semver key to string values; a boolean key
        # falls back to plain ordering instead of feeding a bool column into the
        # string-only semver functions (which would fail at ClickHouse).
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="sentry.is_segment", type=AttributeKey.TYPE_BOOLEAN),
            order_by=TraceItemAttributeValuesRequest.OrderBy(
                sort=TraceItemAttributeValuesRequest.OrderBy.SORT_SEMVER,
            ),
        )
        response = AttributeValuesRequest().execute(message)
        assert response.values == ["true", "false"]
        assert response.counts == [8, 1]

    def test_boolean_existence_check(self, setup_teardown: Any) -> None:
        # `custom_flag` is set on exactly two items (one True, one False); the
        # other items do not have the key. The existence check must exclude the
        # items missing the key, otherwise they'd be miscounted as "false".
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="custom_flag", type=AttributeKey.TYPE_BOOLEAN),
        )
        response = AttributeValuesRequest().execute(message)
        # Equal counts, so ties break on attr_value ASC ("false" before "true").
        assert response.values == ["false", "true"]
        assert response.counts == [1, 1]

    def test_substring_match_on_boolean_rejected(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="sentry.is_segment", type=AttributeKey.TYPE_BOOLEAN),
            value_substring_match="tru",
        )
        with pytest.raises(BadSnubaRPCRequestException):
            AttributeValuesRequest().execute(message)

    def test_unsupported_attribute_type_rejected(self, setup_teardown: Any) -> None:
        message = TraceItemAttributeValuesRequest(
            meta=COMMON_META,
            limit=5,
            key=AttributeKey(name="sentry.duration_ms", type=AttributeKey.TYPE_DOUBLE),
        )
        with pytest.raises(BadSnubaRPCRequestException):
            AttributeValuesRequest().execute(message)
