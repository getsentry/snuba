"""Categorize EAP RPC queries for metrics and cost analysis.

Produces a small set of low-cardinality tags that describe the *shape* of a
request (aggregate vs samples, groupby, formulas, cross-item, filter profile,
etc.). These tags are attached to RPC and routing-strategy metrics so we can
break down latency and bytes-scanned by query type without exploding metric
cardinality.

Design notes
------------
* ``query_type`` is the primary dimension for dashboards (e.g. table_groupby vs
  timeseries_formula vs stats_heatmap).
* Counts are bucketed (``0`` / ``1`` / ``2_3`` / ``gt_3``) rather than raw ints.
* Filter trees are collapsed into a single ``filter_profile`` plus a few cheap
  booleans so we can spot expensive patterns (LIKE, full-text, deep ORs).
* Categorization is best-effort and never raises into the request path.
"""

from __future__ import annotations

from typing import Any

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import TraceItemStatsRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

# Keep every tag value as a short, stable string. Prefer buckets over raw counts
# so Datadog/Sentry metric cardinality stays bounded.
_GROUPBY_BUCKETS = (
    (0, "0"),
    (1, "1"),
    (3, "2_3"),
)
_SELECT_BUCKETS = (
    (1, "1"),
    (5, "2_5"),
    (10, "6_10"),
)
_FILTER_COUNT_BUCKETS = (
    (0, "0"),
    (1, "1"),
    (5, "2_5"),
    (10, "6_10"),
)
_GRANULARITY_BUCKETS_SECS = (
    (60, "lte_1m"),
    (300, "lte_5m"),
    (3600, "lte_1h"),
    (86400, "lte_1d"),
)

# Tags attached to every RPC / routing metric. Keep this set small: each extra
# tag multiplies series count against endpoint/referrer/time_period.
_METRIC_TAG_KEYS = (
    "query_type",
    "trace_item_type",
    "has_groupby",
    "groupby_count",
    "has_formula",
    "has_cross_item",
    "filter_profile",
)


def _bucket(value: int, buckets: tuple[tuple[int, str], ...], overflow: str) -> str:
    for upper, label in buckets:
        if value <= upper:
            return label
    return overflow


def _trace_item_type_name(meta: RequestMeta | None) -> str:
    if meta is None:
        return "none"
    try:
        name = TraceItemType.Name(meta.trace_item_type)
    except ValueError:
        return "unknown"
    # Strip the common prefix so tags stay short: TRACE_ITEM_TYPE_SPAN -> span
    prefix = "TRACE_ITEM_TYPE_"
    if name.startswith(prefix):
        return name[len(prefix) :].lower()
    return name.lower()


def _bool_tag(value: bool) -> str:
    return "true" if value else "false"


def _analyze_filter(item_filter: TraceItemFilter | None) -> dict[str, str]:
    """Walk a TraceItemFilter tree and summarize its cost-relevant shape."""
    if item_filter is None or not item_filter.HasField("value"):
        return {
            "filter_leaf_count": "0",
            "filter_depth": "0",
            "filter_profile": "none",
            "has_or_filter": "false",
            "has_not_filter": "false",
            "has_like_filter": "false",
            "has_search_filter": "false",
        }

    leaf_count = 0
    max_depth = 0
    has_or = False
    has_not = False
    has_like = False
    has_range = False
    has_in = False
    has_exists = False
    has_any_attr = False
    has_equality = False

    def walk(node: TraceItemFilter, depth: int) -> None:
        nonlocal leaf_count, max_depth, has_or, has_not, has_like, has_range
        nonlocal has_in, has_exists, has_any_attr, has_equality

        max_depth = max(max_depth, depth)
        kind = node.WhichOneof("value")
        if kind is None:
            return

        if kind in ("and_filter", "or_filter", "not_filter"):
            if kind == "or_filter":
                has_or = True
            if kind == "not_filter":
                has_not = True
            for child in getattr(node, kind).filters:
                walk(child, depth + 1)
            return

        leaf_count += 1
        if kind == "exists_filter":
            has_exists = True
            return
        if kind == "any_attribute_filter":
            has_any_attr = True
            any_op = node.any_attribute_filter.op
            if any_op in (
                node.any_attribute_filter.OP_LIKE,
                node.any_attribute_filter.OP_NOT_LIKE,
            ):
                has_like = True
            return
        if kind == "comparison_filter":
            op = node.comparison_filter.op
            if op in (ComparisonFilter.OP_LIKE, ComparisonFilter.OP_NOT_LIKE):
                has_like = True
            elif op in (
                ComparisonFilter.OP_LESS_THAN,
                ComparisonFilter.OP_GREATER_THAN,
                ComparisonFilter.OP_LESS_THAN_OR_EQUALS,
                ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
            ):
                has_range = True
            elif op in (
                ComparisonFilter.OP_IN,
                ComparisonFilter.OP_NOT_IN,
                ComparisonFilter.OP_HAS_ANY,
                ComparisonFilter.OP_HAS_ALL,
            ):
                has_in = True
            elif op in (ComparisonFilter.OP_EQUALS, ComparisonFilter.OP_NOT_EQUALS):
                has_equality = True

    walk(item_filter, 1)

    # Single primary profile, ordered from most to least expensive/interesting.
    if has_any_attr:
        profile = "full_text_search"
    elif has_like:
        profile = "like"
    elif has_or and (has_range or has_in or has_exists):
        profile = "or_mixed"
    elif has_or:
        profile = "or_equality"
    elif has_range and (has_in or has_exists or has_equality):
        profile = "mixed"
    elif has_range:
        profile = "range"
    elif has_in:
        profile = "in"
    elif has_exists and not has_equality:
        profile = "exists"
    elif has_equality or has_exists:
        profile = "equality"
    else:
        profile = "other"

    return {
        "filter_leaf_count": _bucket(leaf_count, _FILTER_COUNT_BUCKETS, "gt_10"),
        "filter_depth": _bucket(max_depth, _FILTER_COUNT_BUCKETS, "gt_10"),
        "filter_profile": profile,
        "has_or_filter": _bool_tag(has_or),
        "has_not_filter": _bool_tag(has_not),
        "has_like_filter": _bool_tag(has_like),
        "has_search_filter": _bool_tag(has_any_attr),
    }


def _column_has_formula(column: Column) -> bool:
    return column.WhichOneof("column") in ("formula", "conditional_formula")


def _column_is_aggregate(column: Column) -> bool:
    kind = column.WhichOneof("column")
    if kind in ("aggregation", "conditional_aggregation"):
        return True
    if kind == "formula":
        return _column_is_aggregate(column.formula.left) or _column_is_aggregate(
            column.formula.right
        )
    if kind == "conditional_formula":
        cond = column.conditional_formula
        parts: list[Column] = []
        if cond.HasField("condition"):
            if cond.condition.HasField("left"):
                parts.append(cond.condition.left)
            if cond.condition.HasField("right"):
                parts.append(cond.condition.right)
        if cond.HasField("match"):
            parts.append(cond.match)
        if cond.HasField("default"):
            parts.append(cond.default)
        return any(_column_is_aggregate(part) for part in parts)
    return False


def _expression_has_formula(expression: TimeSeriesExpression) -> bool:
    return expression.WhichOneof("expression") == "formula"


def _categorize_table_request(request: TraceItemTableRequest) -> dict[str, str]:
    columns = list(request.columns)
    has_agg = any(_column_is_aggregate(col) for col in columns)
    has_formula = any(_column_has_formula(col) for col in columns)
    groupby_count = len(request.group_by)
    has_groupby = groupby_count > 0
    has_cross_item = len(request.trace_filters) > 0
    has_agg_filter = request.HasField("aggregation_filter")
    has_limit_by = request.HasField("limit_by")
    has_order_by = len(request.order_by) > 0
    select_count = len(columns)

    if has_cross_item:
        query_type = "table_cross_item"
    elif has_formula and has_groupby:
        query_type = "table_formula_groupby"
    elif has_formula:
        query_type = "table_formula"
    elif has_groupby:
        query_type = "table_groupby"
    elif has_agg:
        query_type = "table_aggregate"
    else:
        query_type = "table_samples"

    tags = {
        "query_type": query_type,
        "has_groupby": _bool_tag(has_groupby),
        "groupby_count": _bucket(groupby_count, _GROUPBY_BUCKETS, "gt_3"),
        "has_formula": _bool_tag(has_formula),
        "has_aggregate": _bool_tag(has_agg),
        "has_cross_item": _bool_tag(has_cross_item),
        "has_agg_filter": _bool_tag(has_agg_filter),
        "has_limit_by": _bool_tag(has_limit_by),
        "has_order_by": _bool_tag(has_order_by),
        "select_count": _bucket(select_count, _SELECT_BUCKETS, "gt_10"),
    }
    filter_tags = _analyze_filter(request.filter if request.HasField("filter") else None)
    tags.update(filter_tags)
    return tags


def _categorize_timeseries_request(request: TimeSeriesRequest) -> dict[str, str]:
    expressions = list(request.expressions)
    # Legacy field; endpoints normally convert aggregations -> expressions first,
    # but categorize defensively in case we're called earlier.
    legacy_aggs = list(request.aggregations)
    has_formula = any(_expression_has_formula(expr) for expr in expressions)
    groupby_count = len(request.group_by)
    has_groupby = groupby_count > 0
    has_cross_item = len(request.trace_filters) > 0
    expr_count = max(len(expressions), len(legacy_aggs))

    if has_cross_item:
        query_type = "timeseries_cross_item"
    elif has_formula and has_groupby:
        query_type = "timeseries_formula_groupby"
    elif has_formula:
        query_type = "timeseries_formula"
    elif has_groupby:
        query_type = "timeseries_groupby"
    else:
        query_type = "timeseries"

    tags = {
        "query_type": query_type,
        "has_groupby": _bool_tag(has_groupby),
        "groupby_count": _bucket(groupby_count, _GROUPBY_BUCKETS, "gt_3"),
        "has_formula": _bool_tag(has_formula),
        # Time series requests are aggregate-by-definition (bucketed expressions).
        "has_aggregate": "true",
        "has_cross_item": _bool_tag(has_cross_item),
        "has_agg_filter": "false",
        "has_limit_by": "false",
        "has_order_by": "false",
        "select_count": _bucket(expr_count, _SELECT_BUCKETS, "gt_10"),
        "granularity": _bucket(
            int(request.granularity_secs or 0), _GRANULARITY_BUCKETS_SECS, "gt_1d"
        ),
    }
    filter_tags = _analyze_filter(request.filter if request.HasField("filter") else None)
    tags.update(filter_tags)
    return tags


def _categorize_stats_request(request: TraceItemStatsRequest) -> dict[str, str]:
    stats_kinds: set[str] = set()
    for stats_type in request.stats_types:
        kind = stats_type.WhichOneof("type")
        if kind is not None:
            stats_kinds.add(kind)

    if stats_kinds == {"heatmap"}:
        query_type = "stats_heatmap"
    elif stats_kinds == {"attribute_distributions"}:
        query_type = "stats_distributions"
    elif len(stats_kinds) > 1:
        query_type = "stats_mixed"
    elif stats_kinds:
        query_type = f"stats_{next(iter(stats_kinds))}"
    else:
        query_type = "stats_unknown"

    tags = {
        "query_type": query_type,
        "has_groupby": "false",
        "groupby_count": "0",
        "has_formula": "false",
        "has_aggregate": "true",
        "has_cross_item": "false",
        "has_agg_filter": "false",
        "has_limit_by": "false",
        "has_order_by": "false",
        "select_count": _bucket(len(request.stats_types), _SELECT_BUCKETS, "gt_10"),
    }
    filter_tags = _analyze_filter(request.filter if request.HasField("filter") else None)
    tags.update(filter_tags)
    return tags


# Endpoint request classes that are not shape-rich still get a stable query_type
# so dashboards can group "everything else" without special-casing missing tags.
_ENDPOINT_QUERY_TYPES: dict[str, str] = {
    "EndpointGetTrace": "get_trace",
    "EndpointGetTraces": "get_traces",
    "EndpointTraceItemDetails": "item_details",
    "EndpointTraceItemAttributeNames": "attribute_names",
    # Class names match RPCEndpoint subclasses (self.__class__.__name__).
    "AttributeValuesRequest": "attribute_values",
    "EndpointDeleteTraceItems": "delete_items",
    "EndpointExportTraceItems": "export_items",
    "CreateSubscriptionRequest": "create_subscription",
}


def _default_shape_tags(query_type: str) -> dict[str, str]:
    return {
        "query_type": query_type,
        "has_groupby": "false",
        "groupby_count": "0",
        "has_formula": "false",
        "has_aggregate": "false",
        "has_cross_item": "false",
        "has_agg_filter": "false",
        "has_limit_by": "false",
        "has_order_by": "false",
        "select_count": "0",
        "filter_leaf_count": "0",
        "filter_depth": "0",
        "filter_profile": "none",
        "has_or_filter": "false",
        "has_not_filter": "false",
        "has_like_filter": "false",
        "has_search_filter": "false",
    }


def extract_query_info(
    in_msg: ProtobufMessage,
    endpoint_name: str | None = None,
) -> dict[str, str]:
    """Full query-shape description (metrics + spans + querylog).

    Safe to call on any protobuf message; unknown request types yield a minimal
    default tag set keyed by endpoint name when available. Never raises.
    """
    try:
        if isinstance(in_msg, TraceItemTableRequest):
            tags = _categorize_table_request(in_msg)
        elif isinstance(in_msg, TimeSeriesRequest):
            tags = _categorize_timeseries_request(in_msg)
        elif isinstance(in_msg, TraceItemStatsRequest):
            tags = _categorize_stats_request(in_msg)
        else:
            query_type = "other"
            if endpoint_name is not None:
                query_type = _ENDPOINT_QUERY_TYPES.get(endpoint_name, "other")
            tags = _default_shape_tags(query_type)

        meta: RequestMeta | None = getattr(in_msg, "meta", None)
        tags["trace_item_type"] = _trace_item_type_name(meta)
        return tags
    except Exception:
        # Categorization must never break the request path.
        tags = _default_shape_tags("categorization_error")
        tags["trace_item_type"] = "unknown"
        return tags


def extract_query_info_tags(
    in_msg: ProtobufMessage,
    endpoint_name: str | None = None,
) -> dict[str, str]:
    """Low-cardinality subset of :func:`extract_query_info` for metric tags."""
    full = extract_query_info(in_msg, endpoint_name=endpoint_name)
    return {key: full[key] for key in _METRIC_TAG_KEYS if key in full}


def extract_query_info_for_log(
    in_msg: ProtobufMessage, endpoint_name: str | None = None
) -> dict[str, Any]:
    """Dict form suitable for embedding in querylog / span data."""
    return dict(extract_query_info(in_msg, endpoint_name=endpoint_name))
