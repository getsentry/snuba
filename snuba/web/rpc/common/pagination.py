"""
This file contains functionality to encode and decode custom page tokens
"""

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemColumnValues,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import TimeWindow


class FlexibleTimeWindowPageWithFilters:

    _TIME_WINDOW_PREFIX = "sentry__time_window"
    _TIME_WINDOW_START_KEY = f"{_TIME_WINDOW_PREFIX}.start_timestamp"
    _TIME_WINDOW_END_KEY = f"{_TIME_WINDOW_PREFIX}.end_timestamp"
    _FILTER_PREFIX = "sentry__filter"

    def __init__(self, page_token: PageToken):
        self._page_token = page_token

    def get_time_window(self) -> TimeWindow | None:
        # breakpoint()
        if not self.page_token.HasField("filter_offset"):
            return None

        start_timestamp = None
        end_timestamp = None
        if self.page_token.filter_offset.HasField("and_filter"):
            for filter in self.page_token.filter_offset.and_filter.filters:
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == self._TIME_WINDOW_START_KEY
                ):
                    start_timestamp = Timestamp(seconds=filter.comparison_filter.value.val_int)
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == self._TIME_WINDOW_END_KEY
                ):
                    end_timestamp = Timestamp(seconds=filter.comparison_filter.value.val_int)

        if not start_timestamp or not end_timestamp:
            raise ValueError("page token does not contain start and end timestamp")

        res = TimeWindow(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        # breakpoint()
        return res

    def get_filters(self) -> Expression | None:
        # iterate through the page token constructed in `create` and return a
        # TraceItemFilter (and_filter) of all the conditions with attributes starting with _FILTER_PREFIX
        # but strip the _FILTER_PREFIX from the attribute key(s)
        if not self.page_token:
            return None
        if not self.page_token.HasField("filter_offset"):
            return None

        column_names: list[str] = []
        column_values: list[Expression] = []

        for filter in self.page_token.filter_offset.and_filter.filters:
            if filter.HasField(
                "comparison_filter"
            ) and filter.comparison_filter.key.name.startswith(self._FILTER_PREFIX):
                if filter.comparison_filter.key.name == f"{self._FILTER_PREFIX}.timestamp":
                    column_names.append("timestamp")
                    column_values.append(f.toDateTime(filter.comparison_filter.value.val_str))
                else:
                    # strip the _FILTER_PREFIX from the attribute key and the dot
                    column_names.append(
                        filter.comparison_filter.key.name[len(self._FILTER_PREFIX) + 1 :]
                    )
                    column_values.append(
                        literal(
                            getattr(
                                filter.comparison_filter.value,
                                str(filter.comparison_filter.value.WhichOneof("value")),
                            )
                        )
                    )
        # Assumes everything in the ORDER BY is ordered by DESC
        # breakpoint()
        if column_names:
            res = f.less(
                f.tuple(*(column(c_name) for c_name in column_names)), f.tuple(*column_values)
            )
            return res
        return None

    @property
    def page_token(self) -> PageToken:
        return self._page_token

    @classmethod
    def create(
        cls,
        in_msg: TraceItemTableRequest,
        time_window: TimeWindow,
        query_results: list[TraceItemColumnValues],
    ) -> "FlexibleTimeWindowPageWithFilters":
        filters = []
        # encode the window
        filters.append(
            TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(name=f"{cls._TIME_WINDOW_PREFIX}.start_timestamp"),
                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                    value=AttributeValue(val_int=time_window.start_timestamp.seconds),
                )
            )
        )
        filters.append(
            TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(name=f"{cls._TIME_WINDOW_PREFIX}.end_timestamp"),
                    op=ComparisonFilter.OP_LESS_THAN,
                    value=AttributeValue(val_int=time_window.end_timestamp.seconds),
                )
            )
        )

        if len(query_results) > 0:
            # create a dict of column.label: last_result_value from query_results
            last_result_values = {
                result_column.attribute_name: result_column.results[-1]
                for result_column in query_results
            }
        else:
            last_result_values = {}
        from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
            attribute_key_to_expression,
        )

        if last_result_values:
            # encode the page token filter conditions
            for order_by_clause in in_msg.order_by:
                last_result_value = last_result_values.get(order_by_clause.column.label, None)
                if last_result_value is not None:
                    # if the field name is `sentry.timestamp`, then handle it differently
                    if order_by_clause.column.label == "sentry.timestamp":
                        # if it's a string, convert it to a datetime and store the integer timestamp in the filter
                        # example format: 2025-10-06 14:00:00
                        # if it's an integer, just store that integer value
                        # otherwise raise a value error
                        timestamp_value = last_result_value.WhichOneof("value")
                        if timestamp_value == "val_str":
                            # parse the string to a datetime and then store the integer timestamp in the filter
                            filters.append(
                                TraceItemFilter(
                                    comparison_filter=ComparisonFilter(
                                        key=AttributeKey(name=f"{cls._FILTER_PREFIX}.timestamp"),
                                        op=ComparisonFilter.OP_LESS_THAN,
                                        value=last_result_value,
                                    )
                                )
                            )
                        else:
                            raise ValueError(
                                f"Timestamp value type {timestamp_value} not supported"
                            )
                    else:
                        # find the attribute in the in_msg.columns attribute that has the same label as the `column` attribute in the order_by_clause
                        # call `attribute_key_to_expression` on it and us its alias as the  name of the AttributeKey in the ComparisonFilter
                        attribute_expression = None
                        for selected_column in in_msg.columns:
                            if selected_column.label == order_by_clause.column.label:
                                attribute_expression = attribute_key_to_expression(
                                    selected_column.key
                                )
                                break
                        if attribute_expression is None:
                            raise ValueError(
                                f"No attribute expression found for column: {order_by_clause.column.label}"
                            )

                        filters.append(
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(
                                        name=f"{cls._FILTER_PREFIX}.{attribute_expression.alias}",
                                    ),
                                    op=ComparisonFilter.OP_LESS_THAN,
                                    value=last_result_value,
                                )
                            )
                        )
                else:
                    raise ValueError(
                        f"No last result value found for column: {order_by_clause.column.label}"
                    )
        # breakpoint()
        return cls(PageToken(filter_offset=TraceItemFilter(and_filter=AndFilter(filters=filters))))


class FlexibleTimeWindowPage:
    _START_TIMESTAMP_KEY = "sentry.start_timestamp"
    _END_TIMESTAMP_KEY = "sentry.end_timestamp"
    _OFFSET_KEY = "sentry.offset"

    def __init__(
        self,
        start_timestamp: Timestamp | None,
        end_timestamp: Timestamp | None,
        offset: int | None = None,
    ):
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.offset = offset

    def encode(self) -> PageToken:
        if self.start_timestamp is not None and self.end_timestamp is not None:
            return PageToken(
                filter_offset=TraceItemFilter(
                    and_filter=AndFilter(
                        filters=[
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(name=self._START_TIMESTAMP_KEY),
                                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                                    value=AttributeValue(val_int=self.start_timestamp.seconds),
                                )
                            ),
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(name=self._END_TIMESTAMP_KEY),
                                    op=ComparisonFilter.OP_LESS_THAN,
                                    value=AttributeValue(val_int=self.end_timestamp.seconds),
                                )
                            ),
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(name=self._OFFSET_KEY),
                                    op=ComparisonFilter.OP_EQUALS,
                                    value=AttributeValue(
                                        val_int=self.offset if self.offset is not None else 0
                                    ),
                                )
                            ),
                        ]
                    )
                )
            )
        else:
            return PageToken(offset=self.offset if self.offset is not None else 0)

    @classmethod
    def decode(cls, page_token: PageToken) -> "FlexibleTimeWindowPage":
        start_timestamp = None
        end_timestamp = None
        offset = None
        if page_token.filter_offset.HasField("and_filter"):
            for filter in page_token.filter_offset.and_filter.filters:
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == cls._START_TIMESTAMP_KEY
                ):
                    start_timestamp = Timestamp(seconds=filter.comparison_filter.value.val_int)
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == cls._END_TIMESTAMP_KEY
                ):
                    end_timestamp = Timestamp(seconds=filter.comparison_filter.value.val_int)
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == cls._OFFSET_KEY
                ):
                    offset = filter.comparison_filter.value.val_int
        elif page_token.HasField("offset"):
            offset = page_token.offset
        return cls(start_timestamp, end_timestamp, offset)
