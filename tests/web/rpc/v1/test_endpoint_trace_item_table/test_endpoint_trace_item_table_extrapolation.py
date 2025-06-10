from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
    Reliability,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    ExistsFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message, write_eap_item

BASE_TIME = datetime.now().replace(
    tzinfo=UTC,
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(minutes=180)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTableWithExtrapolation(BaseApiTest):
    def test_aggregation_on_attribute_column_backward_compat(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        attributes = {
            "custom_tag": AnyValue(string_value="blah"),
        }
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                    }
                    | attributes,
                    server_sample_rate=(
                        1.0 / (2**i)
                    ),  # this results in sampling weights of 1, 2, 4, 8, and 16
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes=attributes,
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="sentry.duration_ms"
                        ),
                        label="count(sentry.duration_ms)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P90,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="p90(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_sum = [v.val_double for v in response.column_values[0].results][0]
        measurement_avg = [v.val_double for v in response.column_values[1].results][0]
        measurement_count_custom_measurement = [
            v.val_double for v in response.column_values[2].results
        ][0]
        measurement_count_duration = [
            v.val_double for v in response.column_values[3].results
        ][0]
        measurement_p90 = [v.val_double for v in response.column_values[4].results][0]
        assert measurement_sum == 98  # weighted sum - 0*1 + 1*2 + 2*4 + 3*8 + 4*16
        assert (
            abs(measurement_avg - 3.16129032) < 0.000001
        )  # weighted average - (0*1 + 1*2 + 2*4 + 3*8 + 4*16) / (1+2+4+8+16)
        assert (
            measurement_count_custom_measurement == 31
        )  # weighted count - 1 + 2 + 4 + 8 + 16
        assert (
            measurement_count_duration == 36
        )  # weighted count (all events have duration) - 5*1 + 1 + 2 + 4 + 8 + 16
        assert abs(measurement_p90 - 4) < 0.01  # weighted p90 - 4

    def test_aggregation_on_attribute_column(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        attributes = {
            "custom_tag": AnyValue(string_value="blah"),
        }
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                    }
                    | attributes,
                    server_sample_rate=(
                        1.0 / (2**i)
                    ),  # this results in sampling weights of 1, 2, 4, 8, and 16
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes=attributes,
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.duration_ms"
                        ),
                        label="count(sentry.duration_ms)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P90,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="p90(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_sum = [v.val_double for v in response.column_values[0].results][0]
        measurement_avg = [v.val_double for v in response.column_values[1].results][0]
        measurement_count_custom_measurement = [
            v.val_double for v in response.column_values[2].results
        ][0]
        measurement_count_duration = [
            v.val_double for v in response.column_values[3].results
        ][0]
        measurement_p90 = [v.val_double for v in response.column_values[4].results][0]
        assert measurement_sum == 98  # weighted sum - 0*1 + 1*2 + 2*4 + 3*8 + 4*16
        assert (
            abs(measurement_avg - 3.16129032) < 0.000001
        )  # weighted average - (0*1 + 1*2 + 2*4 + 3*8 + 4*16) / (1+2+4+8+16)
        assert (
            measurement_count_custom_measurement == 31
        )  # weighted count - 1 + 2 + 4 + 8 + 16
        assert (
            measurement_count_duration == 36
        )  # weighted count (all events have duration) - 5*1 + 1 + 2 + 4 + 8 + 16
        assert abs(measurement_p90 - 4) < 0.01  # weighted p90 - 4

    def test_conditional_aggregation_on_attribute_column(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                        "is_i_divisible_by_2": AnyValue(string_value=str(i % 2 == 0)),
                    },
                    server_sample_rate=(
                        1.0 / (2**i)
                    ),  # this results in sampling weights of 1, 2, 4, 8, and 16
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={"custom_tag": AnyValue(string_value="blah")},
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                        filter=TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING,
                                    name="is_i_divisible_by_2",
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="True"),
                            )
                        ),
                    )
                ),
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                        filter=TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING,
                                    name="is_i_divisible_by_2",
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="False"),
                            )
                        ),
                    )
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_sum = [v.val_double for v in response.column_values[0].results][0]
        measurement_avg = [v.val_double for v in response.column_values[1].results][
            0
        ]  # weighted sum - 0*1 + 1*2 + 2*4 + 3*8 + 4*16

        assert measurement_sum == 72  # weighted sum - 0*1 + 2*4 + 4*16
        assert (
            abs(measurement_avg - 2.6) < 0.000001
        )  # weighted average - (1*2 + 3*8) / (2+8)

    def test_count_reliability_backward_compat(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        attributes = {
            "custom_tag": AnyValue(string_value="blah"),
        }
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                    }
                    | attributes,
                    server_sample_rate=1.0,
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes=attributes,
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=10)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT,
                            name="custom_measurement",
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_count = [v.val_double for v in response.column_values[0].results][0]
        print(measurement_count)
        measurement_reliability = [v for v in response.column_values[0].reliabilities][
            0
        ]
        assert measurement_count == 5
        assert measurement_reliability == Reliability.RELIABILITY_HIGH

    def test_count_reliability(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        attributes = {
            "custom_tag": AnyValue(string_value="blah"),
        }
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                    }
                    | attributes,
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes=attributes,
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_count = [v.val_double for v in response.column_values[0].results][0]
        measurement_reliability = [v for v in response.column_values[0].reliabilities][
            0
        ]
        assert measurement_count == 5
        assert measurement_reliability == Reliability.RELIABILITY_HIGH

    def test_count_reliability_with_group_by_backward_compat(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                        "key": AnyValue(string_value="foo"),
                    },
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "key": AnyValue(string_value="bar"),
                    },
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="key")),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P90,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="p90(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="key")
                    ),
                    descending=True,
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="key"),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)

        measurement_tags = [v.val_str for v in response.column_values[0].results]
        assert measurement_tags == ["foo"]

        measurement_sums = [v.val_double for v in response.column_values[1].results]
        measurement_reliabilities = [v for v in response.column_values[1].reliabilities]
        assert measurement_sums == [sum(range(5))]
        assert measurement_reliabilities == [Reliability.RELIABILITY_HIGH]

        measurement_avgs = [v.val_double for v in response.column_values[2].results]
        measurement_reliabilities = [v for v in response.column_values[2].reliabilities]
        assert len(measurement_avgs) == 1
        assert measurement_avgs[0] == sum(range(5)) / 5
        assert measurement_reliabilities == [Reliability.RELIABILITY_HIGH]

        measurement_counts = [v.val_double for v in response.column_values[3].results]
        measurement_reliabilities = [v for v in response.column_values[3].reliabilities]
        assert measurement_counts == [5]
        assert measurement_reliabilities == [Reliability.RELIABILITY_HIGH]

        measurement_p90s = [v.val_double for v in response.column_values[4].results]
        measurement_reliabilities = [v for v in response.column_values[4].reliabilities]
        assert len(measurement_p90s) == 1
        assert measurement_p90s[0] == 4
        assert measurement_reliabilities == [Reliability.RELIABILITY_LOW]

    def test_count_reliability_with_group_by(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        messages_w_measurement, messages_no_measurement = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            end_timestamp = start_timestamp + timedelta(seconds=1)
            messages_w_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(
                            int_value=i
                        ),  # this results in values of 0, 1, 2, 3, and 4
                        "key": AnyValue(string_value="foo"),
                    },
                    end_timestamp=end_timestamp,
                )
            )
            messages_no_measurement.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "key": AnyValue(string_value="bar"),
                    },
                    end_timestamp=end_timestamp,
                )
            )

        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="key")),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P90,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="p90(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="key")
                    ),
                    descending=True,
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="key"),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)

        print(response)

        measurement_tags = [v.val_str for v in response.column_values[0].results]
        assert measurement_tags == ["foo"]

        measurement_sums = [v.val_double for v in response.column_values[1].results]
        measurement_reliabilities = [v for v in response.column_values[1].reliabilities]
        assert measurement_sums == [sum(range(5))]
        assert measurement_reliabilities == [Reliability.RELIABILITY_HIGH]

        measurement_avgs = [v.val_double for v in response.column_values[2].results]
        measurement_reliabilities = [v for v in response.column_values[2].reliabilities]
        assert len(measurement_avgs) == 1
        assert measurement_avgs[0] == sum(range(5)) / 5
        assert measurement_reliabilities == [Reliability.RELIABILITY_HIGH]

        measurement_counts = [v.val_double for v in response.column_values[3].results]
        measurement_reliabilities = [v for v in response.column_values[3].reliabilities]
        assert measurement_counts == [5]
        assert measurement_reliabilities == [Reliability.RELIABILITY_HIGH]

        measurement_p90s = [v.val_double for v in response.column_values[4].results]
        measurement_reliabilities = [v for v in response.column_values[4].reliabilities]
        assert len(measurement_p90s) == 1
        assert measurement_p90s[0] == 4
        assert measurement_reliabilities == [Reliability.RELIABILITY_LOW]

    def test_formula(self) -> None:
        """
        This test ensures that formulas work with extrapolation.
        Reliabilities will not be returned.
        """
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(
            span_ts,
            {"kyles_measurement": 6},
            server_sample_rate=0.5,
            count=10,
        )
        write_eap_item(
            span_ts,
            raw_attributes={"kyles_measurement": 7},
            count=2,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_DOUBLE, name="kyles_measurement"
                    )
                )
            ),
            columns=[
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="kyles_measurement",
                                ),
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                            ),
                            label="sum(kyles_measurement)",
                        ),
                        right=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="kyles_measurement",
                                ),
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                            ),
                            label="count(kyles_measurement)",
                        ),
                    ),
                    label="sum(kyles_measurement) / count(kyles_measurement)",
                ),
            ],
            limit=1,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="sum(kyles_measurement) / count(kyles_measurement)",
                results=[
                    AttributeValue(val_double=(134 / 22)),
                ],
            ),
        ]

    def test_aggregation_with_nulls(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        messages_a, messages_b = [], []
        for i in range(5):
            start_timestamp = BASE_TIME - timedelta(minutes=i + 1)
            messages_a.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement": AnyValue(int_value=1),
                        "server_sample_rate": AnyValue(double_value=1.0),
                        "custom_tag": AnyValue(string_value="a"),
                    },
                )
            )
            messages_b.append(
                gen_item_message(
                    start_timestamp=start_timestamp,
                    attributes={
                        "custom_measurement2": AnyValue(int_value=1),
                        "server_sample_rate": AnyValue(double_value=1.0),
                        "custom_tag": AnyValue(string_value="b"),
                    },
                )
            )
        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_a + messages_b,
        )

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=10)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="custom_tag")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement2"
                        ),
                        label="sum(custom_measurement2)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="custom_tag"),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="custom_tag"
                        )
                    ),
                ),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="custom_tag",
                results=[AttributeValue(val_str="a"), AttributeValue(val_str="b")],
            ),
            TraceItemColumnValues(
                attribute_name="sum(custom_measurement)",
                results=[AttributeValue(val_double=5), AttributeValue(is_null=True)],
                reliabilities=[
                    Reliability.RELIABILITY_HIGH,
                    Reliability.RELIABILITY_UNSPECIFIED,
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(custom_measurement2)",
                results=[AttributeValue(is_null=True), AttributeValue(val_double=5)],
                reliabilities=[
                    Reliability.RELIABILITY_UNSPECIFIED,
                    Reliability.RELIABILITY_HIGH,
                ],
            ),
        ]
