"""
This file contains functionality to encode and decode custom page tokens
"""

from collections.abc import Mapping
from typing import Final

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

from snuba.protos.common import NORMALIZED_COLUMNS_EAP_ITEMS
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression, OptionalScalarType
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    semver_sort_key,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import TimeWindow

# Value an absent map-backed attribute sorts as, per attribute type. The page boundary
# compares the ORDER BY values as a tuple, and a NULL element makes the whole comparison
# NULL, which drops the row instead of paginating past it. A stored value equal to the
# sentinel ties with an absent one, which is harmless: the trailing `sentry.item_id`
# element breaks the tie.
_NULL_ORDERING_SENTINELS: Final[Mapping[AttributeKey.Type.ValueType, OptionalScalarType]] = {
    AttributeKey.Type.TYPE_BOOLEAN: False,
    AttributeKey.Type.TYPE_DOUBLE: 0.0,
    AttributeKey.Type.TYPE_FLOAT: 0.0,
    AttributeKey.Type.TYPE_INT: 0,
    AttributeKey.Type.TYPE_STRING: "",
}


def null_safe_ordering_expression(
    expression: Expression, attr_type: AttributeKey.Type.ValueType
) -> Expression:
    """Make an absent map-backed attribute sort as its type's zero value.

    Apply this to the ORDER BY and to the page boundary of the same column, or the two
    disagree on where absent keys sort and pagination skips or repeats rows. A type with no
    sentinel is returned unchanged, which covers the columns that need none: normalized
    columns and `timestamp` (never NULL, and their page token carries no type), and arrays
    (rejected from ORDER BY upstream).
    """
    if attr_type not in _NULL_ORDERING_SENTINELS:
        return expression
    return f.ifNull(expression, literal(_NULL_ORDERING_SENTINELS[attr_type]))


def _comparison_value_expression(comparison_filter: ComparisonFilter) -> Expression:
    value = comparison_filter.value
    if value.is_null or value.WhichOneof("value") == "val_null":
        if comparison_filter.key.type not in _NULL_ORDERING_SENTINELS:
            raise BadSnubaRPCRequestException(
                f"page token column {comparison_filter.key.name} is null and has no type to sort it by"
            )
        return literal(_NULL_ORDERING_SENTINELS[comparison_filter.key.type])
    return literal(getattr(value, str(value.WhichOneof("value"))))


class FlexibleTimeWindowPageWithFilters:
    _TIME_WINDOW_PREFIX = "sentry__time_window"
    _TIME_WINDOW_START_KEY = f"{_TIME_WINDOW_PREFIX}.start_timestamp"
    _TIME_WINDOW_END_KEY = f"{_TIME_WINDOW_PREFIX}.end_timestamp"
    _FILTER_PREFIX = "sentry__filter"
    # Marks a page-boundary column whose ORDER BY used SORT_SEMVER, so
    # get_filters applies the semver key on both sides of the comparison.
    _SEMVER_FILTER_PREFIX = "sentry__semver_filter"

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
        # SORT_SEMVER, so the boundary comparison must use the semver key too.
        column_is_semver: list[bool] = []
        # Parallel to column_names: the attribute type, which tells the boundary
        # comparison how a map-backed column with an absent key was sorted.
        column_types: list[AttributeKey.Type.ValueType] = []

        for filter in self.page_token.filter_offset.and_filter.filters:
            if not filter.HasField("comparison_filter"):
                continue
            key_name = filter.comparison_filter.key.name
            is_semver = key_name.startswith(f"{self._SEMVER_FILTER_PREFIX}.")
            is_regular = key_name.startswith(f"{self._FILTER_PREFIX}.")
            if not (is_semver or is_regular):
                continue

            if key_name == f"{self._FILTER_PREFIX}.timestamp":
                # Resolve the value first and raise on an unsupported type
                # (tokens are client-supplied) so the parallel lists stay in
                # sync and the strict zip() below can't crash. Mirrors create().
                value = filter.comparison_filter.value
                if value.HasField("val_str"):
                    column_values.append(f.toDateTime(value.val_str))
                elif value.HasField("val_double"):
                    column_values.append(literal(value.val_double))
                elif value.HasField("val_int"):
                    column_values.append(literal(value.val_int))
                else:
                    raise ValueError(
                        f"Timestamp value type {value.WhichOneof('value')} not supported "
                        "in page token"
                    )
                column_names.append("timestamp")
                column_is_semver.append(False)
                column_types.append(AttributeKey.Type.TYPE_UNSPECIFIED)
            else:
                # strip the matching prefix (and the dot) to recover the alias
                prefix = self._SEMVER_FILTER_PREFIX if is_semver else self._FILTER_PREFIX
                column_names.append(key_name[len(prefix) + 1 :])
                column_is_semver.append(is_semver)
                column_types.append(filter.comparison_filter.key.type)
                column_values.append(_comparison_value_expression(filter.comparison_filter))
        # Assumes everything in the ORDER BY is ordered by DESC
        if column_names:
            col_exprs = []
            val_exprs = []
            for c_name, c_value, is_semver, c_type in zip(
                column_names, column_values, column_is_semver, column_types, strict=True
            ):
                # An absent map-backed key sorts as its type's zero value, as in ORDER BY.
                col_expr = null_safe_ordering_expression(column(c_name), c_type)
                # For SORT_SEMVER columns, apply the same semver key on both sides
                # so the page-boundary comparison uses the same ordering as ORDER BY.
                if is_semver:
                    col_exprs.append(semver_sort_key(col_expr))
                    val_exprs.append(semver_sort_key(c_value))
                else:
                    col_exprs.append(col_expr)
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

                        # Mark SORT_SEMVER string columns so get_filters wraps
                        # both sides in the semver key (matching ORDER BY); mirrors
                        # the resolver's string-only guard.
                        is_semver = (
                            order_by_clause.sort == TraceItemTableRequest.OrderBy.SORT_SEMVER
                            and selected_key is not None
                            and selected_key.type == AttributeKey.TYPE_STRING
                        )
                        prefix = cls._SEMVER_FILTER_PREFIX if is_semver else cls._FILTER_PREFIX

                        # Only a map-backed attribute reads as NULL when its key is absent,
                        # and `last_result_value` is then null; get_filters uses this type to
                        # sort it the way ORDER BY did. A normalized column is never NULL and
                        # its ORDER BY compares the raw column, so leaving the type unset is
                        # what keeps the two sides in step.
                        null_sort_type = (
                            selected_key.type
                            if selected_key is not None
                            and selected_key.name not in NORMALIZED_COLUMNS_EAP_ITEMS
                            else AttributeKey.Type.TYPE_UNSPECIFIED
                        )

                        filters.append(
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(
                                        name=f"{prefix}.{attribute_expression.alias}",
                                        type=null_sort_type,
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
