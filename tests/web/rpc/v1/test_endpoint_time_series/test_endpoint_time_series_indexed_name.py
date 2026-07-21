from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.common import USE_INDEXED_NAME_ORGANIZATION_IDS_OPTION
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.utcnow().replace(
    hour=8, minute=0, second=0, microsecond=0, tzinfo=UTC
) - timedelta(hours=24)

GRANULARITY_SECS = 300
QUERY_DURATION = 3600

# A matching metric is written this many times; each carries value=1.0, so a SUM
# over the matching items totals MATCHING_COUNT.
MATCHING_COUNT = 6
NON_MATCHING_COUNT = 4


def _store_metrics() -> None:
    """Write MATCHING_COUNT my.metric + NON_MATCHING_COUNT other.metric items.
    Ingestion promotes sentry.metric.name into indexed_name."""
    messages: list[bytes] = []
    for name, count in (("my.metric", MATCHING_COUNT), ("other.metric", NON_MATCHING_COUNT)):
        for _ in range(count):
            messages.append(
                gen_item_message(
                    start_timestamp=BASE_TIME + timedelta(seconds=60),
                    type=TraceItemType.TRACE_ITEM_TYPE_METRIC,
                    remove_default_attributes=True,
                    attributes={
                        "sentry.metric.name": AnyValue(string_value=name),
                        "value": AnyValue(double_value=1.0),
                    },
                )
            )
    write_raw_unprocessed_events(get_storage(StorageKey("eap_items")), messages)  # type: ignore


def _request() -> TimeSeriesRequest:
    """SUM(value) over metrics named ``my.metric`` (scoped to organization_id=1)."""
    return TimeSeriesRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
            end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp()) + QUERY_DURATION),
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_METRIC,
        ),
        filter=TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.metric.name"),
                op=ComparisonFilter.OP_EQUALS,
                value=AttributeValue(val_str="my.metric"),
            )
        ),
        aggregations=[
            AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="value"),
                label="sum",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
            ),
        ],
        granularity_secs=GRANULARITY_SECS,
    )


def _total(response: TimeSeriesResponse) -> float:
    return float(sum(dp.data for ts in response.result_timeseries for dp in ts.data_points))


@pytest.mark.eap
@pytest.mark.redis_db
class TestTimeSeriesIndexedName(BaseApiTest):
    def test_metric_name_filter_with_indexed_name_enabled(self) -> None:
        """A metric-name-filtered time series returns the right rows with the org enabled."""
        _store_metrics()

        with override_options("snuba", {USE_INDEXED_NAME_ORGANIZATION_IDS_OPTION: [1]}):
            response = EndpointTimeSeries().execute(_request())

        # Only the my.metric items (value=1.0 each) match; other.metric is excluded.
        assert _total(response) == float(MATCHING_COUNT)

    def test_indexed_name_rewrite_is_result_preserving(self) -> None:
        """Results are identical with the org disabled (bucket) and enabled (indexed_name)."""
        _store_metrics()

        with override_options("snuba", {USE_INDEXED_NAME_ORGANIZATION_IDS_OPTION: []}):
            disabled = EndpointTimeSeries().execute(_request())

        with override_options("snuba", {USE_INDEXED_NAME_ORGANIZATION_IDS_OPTION: [1]}):
            enabled = EndpointTimeSeries().execute(_request())

        assert _total(disabled) == float(MATCHING_COUNT)
        assert list(enabled.result_timeseries) == list(disabled.result_timeseries)
