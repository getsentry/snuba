import uuid
from datetime import datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ExistsFilter, TraceItemFilter
from sentry_protos.snuba.v1.trace_item_table_column_pb2 import Column

from snuba.web.rpc.common.debug_info import setup_trace_query_settings
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable

BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestDebugInfo:
    @staticmethod
    def _create_trace_item_table_request(debug: bool = False) -> TraceItemTableRequest:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        request_id = str(uuid.uuid4())

        return TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id=request_id,
                debug=debug,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="server_name")
                )
            ],
            limit=1,
        )

    def test_debug_info_present_when_requested(self) -> None:
        debug_request = self._create_trace_item_table_request(debug=True)
        debug_response = EndpointTraceItemTable().execute(debug_request)
        assert debug_response.meta.request_id == debug_request.meta.request_id
        assert len(debug_response.meta.query_info) > 0
        for query_info in debug_response.meta.query_info:
            assert query_info.stats is not None
            assert query_info.metadata is not None
            assert query_info.trace_logs is not None
            assert isinstance(query_info.trace_logs, str)

    def test_debug_info_absent_when_not_requested(self) -> None:
        non_debug_request = self._create_trace_item_table_request(debug=False)
        non_debug_response = EndpointTraceItemTable().execute(non_debug_request)
        assert non_debug_response.meta.request_id == non_debug_request.meta.request_id
        assert len(non_debug_response.meta.query_info) == 0

    def test_trace_query_settings(self) -> None:
        settings = setup_trace_query_settings()
        assert settings.get_clickhouse_settings() == {
            "send_logs_level": "trace",
            "log_profile_events": 1,
        }
