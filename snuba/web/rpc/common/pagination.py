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

from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import TimeWindow


class AttrValue:
    def __init__(self, value: str | int | float | bool):
        self.value = value

    def to_attribute_value(self) -> AttributeValue:
        if isinstance(self.value, str):
            return AttributeValue(val_str=self.value)
        elif isinstance(self.value, int):
            return AttributeValue(val_int=self.value)
        elif isinstance(self.value, float):
            return AttributeValue(val_double=self.value)
        elif isinstance(self.value, bool):
            return AttributeValue(val_bool=self.value)
        else:
            raise ValueError(f"Unsupported attribute value type: {type(self.value)}")

    def get_attribute_key_type(self) -> AttributeKey.Type.ValueType:
        if isinstance(self.value, str):
            return AttributeKey.Type.TYPE_STRING
        elif isinstance(self.value, int):
            return AttributeKey.Type.TYPE_INT
        elif isinstance(self.value, float):
            return AttributeKey.Type.TYPE_DOUBLE
        elif isinstance(self.value, bool):
            return AttributeKey.Type.TYPE_BOOL
        else:
            raise ValueError(f"Unsupported attribute value type: {type(self.value)}")


class FlexibleTimeWindowPageWithFilters:

    _TIME_WINDOW_PREFIX = "sentry__time_window"
    _TIME_WINDOW_START_KEY = f"{_TIME_WINDOW_PREFIX}.start_timestamp"
    _TIME_WINDOW_END_KEY = f"{_TIME_WINDOW_PREFIX}.end_timestamp"
    _FILTER_PREFIX = "sentry__filter"

    def __init__(self, page_token: PageToken):
        self._page_token = page_token

    def get_time_window(self) -> TimeWindow:
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

        return TimeWindow(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

    def get_filters(self) -> TraceItemFilter:
        # iterate through the page token constructed in `create` and return a
        # TraceItemFilter (and_filter) of all the conditions with attributes starting with _FILTER_PREFIX
        # but strip the _FILTER_PREFIX from the attribute key(s)
        filters = []
        for filter in self.page_token.filter_offset.and_filter.filters:
            if filter.HasField(
                "comparison_filter"
            ) and filter.comparison_filter.key.name.startswith(self._FILTER_PREFIX):
                # strip the _FILTER_PREFIX from the attribute key and the dot
                # copy the entire filter and then modify just the key name
                new_filter = TraceItemFilter()
                new_filter.CopyFrom(filter)
                new_filter.comparison_filter.key.name = filter.comparison_filter.key.name[
                    len(self._FILTER_PREFIX) + 1 :
                ]
                filters.append(new_filter)
        return TraceItemFilter(and_filter=AndFilter(filters=filters))

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

        # encode the page token filter conditions
        for order_by_clause in in_msg.order_by:
            if order_by_clause.column.key.name == "sentry.item_id":
                # item_id uniquely identifies a trace item and so we always compare with less than or greater than
                comparison_op = (
                    ComparisonFilter.OP_LESS_THAN
                    if order_by_clause.descending
                    else ComparisonFilter.OP_GREATER_THAN
                )
                filters.append(
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                name=f"{cls._TIME_WINDOW_PREFIX}.end_timestamp",
                                type=AttributeKey.Type.TYPE_STRING,
                            ),
                            op=ComparisonFilter.OP_LESS_THAN,
                            value=AttributeValue(val_int=time_window.end_timestamp.seconds),
                        )
                    )
                )
            else:
                last_result_value = last_result_values.get(order_by_clause.column.label, None)
                if last_result_value is not None:
                    # every other value besides item_id does not guarantee uniqueness and so we compare with less than or equal to or greater than or equal to
                    comparison_op = (
                        ComparisonFilter.OP_LESS_THAN_OR_EQUALS
                        if order_by_clause.descending
                        else ComparisonFilter.OP_GREATER_THAN_OR_EQUALS
                    )
                    filters.append(
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    name=f"{cls._FILTER_PREFIX}.{order_by_clause.column.label}",
                                    type=AttrValue(
                                        value=last_result_value  # type: ignore
                                    ).get_attribute_key_type(),
                                ),
                                op=comparison_op,
                                value=AttrValue(value=last_result_value).to_attribute_value(),  # type: ignore
                            )
                        )
                    )
                else:
                    raise ValueError(
                        f"No last result value found for column: {order_by_clause.column.label}"
                    )

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
