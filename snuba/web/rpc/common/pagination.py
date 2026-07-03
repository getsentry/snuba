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
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    semver_sort_key,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import TimeWindow


class FlexibleTimeWindowPageWithFilters:
    _TIME_WINDOW_PREFIX = "sentry__time_window"
    _TIME_WINDOW_START_KEY = f"{_TIME_WINDOW_PREFIX}.start_timestamp"
    _TIME_WINDOW_END_KEY = f"{_TIME_WINDOW_PREFIX}.end_timestamp"
    _FILTER_PREFIX = "sentry__filter"
    # Marks a page-boundary column whose ORDER BY used SORT_NATURAL (semver), so
    # get_filters applies the same semver key on both sides of the comparison.
    _NATURAL_FILTER_PREFIX = "sentry__natural_filter"

    def __init__(self, page_token: PageToken):
        self._page_token = page_token

    def get_time_window(self) -> TimeWindow | None:
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
        # Parallel to column_names: True when that column's ORDER BY used
        # SORT_NATURAL, so the boundary comparison must use the semver key too.
        column_is_natural: list[bool] = []

        for filter in self.page_token.filter_offset.and_filter.filters:
            if not filter.HasField("comparison_filter"):
                continue
            key_name = filter.comparison_filter.key.name
            is_natural = key_name.startswith(f"{self._NATURAL_FILTER_PREFIX}.")
            is_regular = key_name.startswith(f"{self._FILTER_PREFIX}.")
            if not (is_natural or is_regular):
                continue

            if key_name == f"{self._FILTER_PREFIX}.timestamp":
                column_names.append("timestamp")
                column_is_natural.append(False)
                if filter.comparison_filter.value.HasField("val_str"):
                    column_values.append(f.toDateTime(filter.comparison_filter.value.val_str))
                elif filter.comparison_filter.value.HasField("val_double"):
                    column_values.append(literal(filter.comparison_filter.value.val_double))
                elif filter.comparison_filter.value.HasField("val_int"):
                    column_values.append(literal(filter.comparison_filter.value.val_int))
            else:
                # strip the matching prefix (and the dot) to recover the alias
                prefix = self._NATURAL_FILTER_PREFIX if is_natural else self._FILTER_PREFIX
                column_names.append(key_name[len(prefix) + 1 :])
                column_is_natural.append(is_natural)
                column_values.append(
                    literal(
                        getattr(
                            filter.comparison_filter.value,
                            str(filter.comparison_filter.value.WhichOneof("value")),
                        )
                    )
                )
        # Assumes everything in the ORDER BY is ordered by DESC
        if column_names:
            col_exprs = []
            val_exprs = []
            for c_name, c_value, is_natural in zip(
                column_names, column_values, column_is_natural, strict=True
            ):
                # For SORT_NATURAL columns, apply the same semver key on both sides
                # so the page-boundary comparison uses the same ordering as ORDER BY.
                if is_natural:
                    col_exprs.append(semver_sort_key(column(c_name)))
                    val_exprs.append(semver_sort_key(c_value))
                else:
                    col_exprs.append(column(c_name))
                    val_exprs.append(c_value)
            res = f.less(f.tuple(*col_exprs), f.tuple(*val_exprs))
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
                        if timestamp_value in ("val_str", "val_double", "val_int"):
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
                        selected_key = None
                        for selected_column in in_msg.columns:
                            if selected_column.label == order_by_clause.column.label:
                                attribute_expression = attribute_key_to_expression(
                                    selected_column.key
                                )
                                selected_key = selected_column.key
                                break
                        if attribute_expression is None:
                            raise ValueError(
                                f"No attribute expression found for column: {order_by_clause.column.label}"
                            )

                        # Mark the column as SORT_NATURAL (semver) when the request
                        # ordered it that way, so get_filters wraps both sides in the
                        # semver key and the page boundary matches the ORDER BY. Mirror
                        # the resolver's guard: only string columns get the semver key.
                        is_natural = (
                            order_by_clause.sort
                            == TraceItemTableRequest.OrderBy.SORT_NATURAL
                            and selected_key is not None
                            and selected_key.type == AttributeKey.TYPE_STRING
                        )
                        prefix = cls._NATURAL_FILTER_PREFIX if is_natural else cls._FILTER_PREFIX

                        filters.append(
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(
                                        name=f"{prefix}.{attribute_expression.alias}",
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
        return cls(PageToken(filter_offset=TraceItemFilter(and_filter=AndFilter(filters=filters))))
