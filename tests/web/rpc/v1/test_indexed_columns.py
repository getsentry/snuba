"""EAP-620: `sentry.session_id` and `gen_ai.conversation.id` resolve to their indexed
physical columns (`session_id`, `ai_conversation_id`) as bare columns, so `=` / `IN <literal>`
filters prune granules via the bloom-filter skip index (`bf_session_id`,
`bf_ai_conversation_id`). These tests execute a real `TraceItemTable` query, then run
`EXPLAIN indexes = 1` on the generated ClickHouse SQL to assert the skip index is applied,
and check the filter returns the right rows.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    StrArray,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    ExistsFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.helpers import write_raw_unprocessed_events

# Two items: item A (session/conversation "a", color red), item B ("b", color blue).
SESSION_A = "11111111-1111-1111-1111-111111111111"
SESSION_B = "22222222-2222-2222-2222-222222222222"
CONV_A = "conv-a"
CONV_B = "conv-b"


def _item_message(ts: datetime, session_id: str, conversation_id: str, color: str) -> bytes:
    item_ts = Timestamp()
    item_ts.FromDatetime(ts)
    received = Timestamp()
    received.GetCurrentTime()
    return TraceItem(
        organization_id=1,
        project_id=1,
        item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        timestamp=item_ts,
        trace_id=uuid.uuid4().hex,
        item_id=uuid.uuid4().int.to_bytes(16, "little"),
        received=received,
        retention_days=90,
        server_sample_rate=1.0,
        session_id=session_id,
        conversation_id=conversation_id,
        attributes={
            "color": AnyValue(string_value=color),
            "sentry.end_timestamp_precise": AnyValue(
                double_value=(ts + timedelta(seconds=1)).timestamp()
            ),
            "sentry.received": AnyValue(double_value=received.seconds),
            "sentry.start_timestamp_precise": AnyValue(double_value=ts.timestamp()),
            "start_timestamp_ms": AnyValue(double_value=int(ts.timestamp() * 1000)),
        },
    ).SerializeToString()


@pytest.mark.eap
@pytest.mark.redis_db
class TestIndexedColumnSkipIndex:
    @pytest.fixture(autouse=True)
    def setup(self, eap: None, redis_db: None) -> None:
        self.base_time = datetime.now(tz=UTC).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=1)
        self.start_ts = Timestamp(seconds=int((self.base_time - timedelta(hours=1)).timestamp()))
        self.end_ts = Timestamp(seconds=int((self.base_time + timedelta(hours=2)).timestamp()))
        messages = [
            _item_message(self.base_time, SESSION_A, CONV_A, "red"),
            _item_message(self.base_time + timedelta(minutes=1), SESSION_B, CONV_B, "blue"),
            # "green" sets neither: an unset conversation_id ingests as an empty
            # ai_conversation_id, so exists on gen_ai.conversation.id must exclude it.
            # (session_id is intentionally not exercised for exists: an unset session_id
            # ingests as a random non-nil UUID, so absence is indistinguishable there.)
            _item_message(self.base_time + timedelta(minutes=2), "", "", "green"),
        ]
        write_raw_unprocessed_events(get_writable_storage(StorageKey("eap_items")), messages)

    def _execute(self, filt: TraceItemFilter) -> tuple[str, list[str]]:
        """Run a TraceItemTable query in debug mode; return (generated_sql, sorted colors)."""
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test",
                start_timestamp=self.start_ts,
                end_timestamp=self.end_ts,
                request_id=uuid.uuid4().hex,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                debug=True,
            ),
            filter=filt,
            columns=[Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color"))],
            limit=100,
        )
        response = EndpointTraceItemTable().execute(message)
        sql = response.meta.query_info[0].metadata.sql
        colors = (
            sorted(r.val_str for r in response.column_values[0].results)
            if response.column_values
            else []
        )
        return sql, colors

    @staticmethod
    def _explain_uses_index(sql: str, index_name: str) -> bool:
        conn = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM).get_query_connection(
            ClickhouseClientSettings.QUERY
        )
        plan = "\n".join(str(row[0]) for row in conn.execute(f"EXPLAIN indexes = 1 {sql}").results)
        # The index only appears in the plan's "Skip" section when ClickHouse can analyze the
        # condition against it — i.e. when the column is bare. A value-changing wrapper
        # (Nullable cast, replaceAll, ...) drops it from the plan. (With a single granule
        # nothing is pruned, so we assert the index is *applied to the plan*, not a count.)
        return index_name in plan

    # honestly this test is probably unnecessary. It was useful when I was developing, so I figured why not merge it in.
    @pytest.mark.parametrize(
        "attribute_name,op,value,expected_index,expected_colors",
        [
            (
                "sentry.session_id",
                ComparisonFilter.OP_EQUALS,
                AttributeValue(val_str=SESSION_A),
                "bf_session_id",
                ["red"],
            ),
            (
                "sentry.session_id",
                ComparisonFilter.OP_IN,
                AttributeValue(val_str_array=StrArray(values=[SESSION_A, SESSION_B])),
                "bf_session_id",
                ["blue", "red"],
            ),
            (
                "gen_ai.conversation.id",
                ComparisonFilter.OP_EQUALS,
                AttributeValue(val_str=CONV_A),
                "bf_ai_conversation_id",
                ["red"],
            ),
            (
                "gen_ai.conversation.id",
                ComparisonFilter.OP_IN,
                AttributeValue(val_str_array=StrArray(values=[CONV_A, CONV_B])),
                "bf_ai_conversation_id",
                ["blue", "red"],
            ),
        ],
    )
    def test_filter_uses_skip_index_and_matches(
        self,
        attribute_name: str,
        op: "ComparisonFilter.Op.ValueType",
        value: AttributeValue,
        expected_index: str,
        expected_colors: list[str],
    ) -> None:
        sql, colors = self._execute(
            TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name=attribute_name),
                    op=op,
                    value=value,
                )
            )
        )
        assert colors == expected_colors
        assert self._explain_uses_index(sql, expected_index), sql

    def test_exists_on_conversation_id_excludes_rows_without_a_value(self) -> None:
        # gen_ai.conversation.id is optional: the "green" span never set it, so its
        # ai_conversation_id is empty. exists() must treat empty as absent and exclude
        # it, rather than matching every row (a non-nullable column is never NULL, so a
        # plain isNotNull check would always be true).
        _, colors = self._execute(
            TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="gen_ai.conversation.id")
                )
            )
        )
        assert colors == ["blue", "red"]
