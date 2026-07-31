"""Tests that `sentry.timestamp` range filters on the TimeSeries endpoint are
emitted against the raw `timestamp` DateTime column (never CAST to Float64) and
that conditions identical to the mandatory time-range bound are deduped.

These build the query AST directly via `build_query` so we can assert on the
generated WHERE clause without needing a live ClickHouse.
"""

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series import build_query

# Aligned to a 600s boundary so the meta window matches what the time-series
# bucketing produces; values are arbitrary fixed unix seconds.
START_TS = 1781040600
END_TS = 1781127600

_RANGE_OPS = {"less", "lessOrEquals", "greater", "greaterOrEquals"}


def _flatten_and(exp: Expression | None) -> list[Expression]:
    if isinstance(exp, FunctionCall) and exp.function_name == "and":
        out: list[Expression] = []
        for param in exp.parameters:
            out.extend(_flatten_and(param))
        return out
    return [] if exp is None else [exp]


def _is_raw_timestamp_column(exp: Expression) -> bool:
    return isinstance(exp, Column) and exp.column_name == "timestamp"


def _timestamp_range_conditions(condition: Expression | None) -> list[FunctionCall]:
    """All top-level range comparisons whose left side is the raw `timestamp` column."""
    conditions = []
    for conjunct in _flatten_and(condition):
        if (
            isinstance(conjunct, FunctionCall)
            and conjunct.function_name in _RANGE_OPS
            and conjunct.parameters
            and _is_raw_timestamp_column(conjunct.parameters[0])
        ):
            conditions.append(conjunct)
    return conditions


def _contains_cast_of_timestamp(exp: Expression | None) -> bool:
    """True if any `cast(timestamp, ...)` (the Float64 form) appears in the tree."""
    if isinstance(exp, FunctionCall):
        if (
            exp.function_name == "cast"
            and exp.parameters
            and _is_raw_timestamp_column(exp.parameters[0])
        ):
            return True
        return any(_contains_cast_of_timestamp(p) for p in exp.parameters)
    return False


def _make_request(*timestamp_filters: TraceItemFilter) -> TimeSeriesRequest:
    request_filter = None
    if len(timestamp_filters) == 1:
        request_filter = timestamp_filters[0]
    elif len(timestamp_filters) > 1:
        request_filter = TraceItemFilter(and_filter=AndFilter(filters=list(timestamp_filters)))

    return TimeSeriesRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=START_TS),
            end_timestamp=Timestamp(seconds=END_TS),
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        aggregations=[
            AttributeAggregation(
                aggregate=Function.FUNCTION_COUNT,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration"),
                label="count",
            ),
        ],
        granularity_secs=600,
        filter=request_filter or TraceItemFilter(),
    )


def _ts_filter(op: "ComparisonFilter.Op.ValueType", seconds: int) -> TraceItemFilter:
    return TraceItemFilter(
        comparison_filter=ComparisonFilter(
            key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.timestamp"),
            op=op,
            value=AttributeValue(val_double=seconds),
        )
    )


def test_no_timestamp_filter_keeps_only_mandatory_bounds() -> None:
    query = build_query(_make_request())
    conds = _timestamp_range_conditions(query.get_condition())
    # Just the mandatory lower + upper bound from the request time window.
    assert len(conds) == 2
    assert not _contains_cast_of_timestamp(query.get_condition())


def test_identical_range_is_deduped_to_one() -> None:
    # A sentry.timestamp filter whose bounds exactly match the meta time window.
    # Using `>=` for the lower bound and `<` for the upper bound mirrors the
    # mandatory condition, so the two pairs are byte-identical and collapse.
    query = build_query(
        _make_request(
            _ts_filter(ComparisonFilter.OP_GREATER_THAN_OR_EQUALS, START_TS),
            _ts_filter(ComparisonFilter.OP_LESS_THAN, END_TS),
        )
    )
    conds = _timestamp_range_conditions(query.get_condition())
    # The duplicate lower/upper bounds are pruned -> only one of each remains.
    assert len(conds) == 2
    functions = sorted(c.function_name for c in conds)
    assert functions == ["greaterOrEquals", "less"]
    assert not _contains_cast_of_timestamp(query.get_condition())


def test_different_range_keeps_both_bounds_as_intersection() -> None:
    # A tighter window than the meta range: both the meta bounds and the filter
    # bounds survive; ClickHouse's AND yields the smaller (intersected) window.
    query = build_query(
        _make_request(
            _ts_filter(ComparisonFilter.OP_GREATER_THAN_OR_EQUALS, START_TS + 600),
            _ts_filter(ComparisonFilter.OP_LESS_THAN, END_TS - 600),
        )
    )
    conds = _timestamp_range_conditions(query.get_condition())
    # 2 mandatory + 2 distinct filter bounds, none collapsed.
    assert len(conds) == 4
    assert not _contains_cast_of_timestamp(query.get_condition())


def test_more_than_two_distinct_bounds_all_kept_and_not_float() -> None:
    # Three distinct sentry.timestamp bounds (none equal to a meta bound) plus the
    # two mandatory bounds: all are kept, and crucially none use CAST(.. Float64).
    query = build_query(
        _make_request(
            _ts_filter(ComparisonFilter.OP_GREATER_THAN, START_TS + 100),
            _ts_filter(ComparisonFilter.OP_GREATER_THAN_OR_EQUALS, START_TS + 600),
            _ts_filter(ComparisonFilter.OP_LESS_THAN, END_TS - 600),
        )
    )
    condition = query.get_condition()
    conds = _timestamp_range_conditions(condition)
    assert len(conds) == 5
    # Every timestamp comparison compares the raw DateTime column, not a Float cast.
    assert not _contains_cast_of_timestamp(condition)
    for cond in conds:
        assert _is_raw_timestamp_column(cond.parameters[0])
