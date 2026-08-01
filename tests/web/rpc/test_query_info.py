from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    AttributeDistributionsRequest,
    HeatmapRequest,
    StatsType,
    TraceItemStatsRequest,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    AnyAttributeFilter,
    ComparisonFilter,
    ExistsFilter,
    OrFilter,
    TraceItemFilter,
)

from snuba.web.rpc.common.query_info import extract_query_info, extract_query_info_tags


def _meta(item_type: TraceItemType.ValueType = TraceItemType.TRACE_ITEM_TYPE_SPAN) -> RequestMeta:
    return RequestMeta(
        project_ids=[1],
        organization_id=1,
        referrer="test",
        trace_item_type=item_type,
    )


def _attr(name: str = "sentry.duration_ms") -> AttributeKey:
    return AttributeKey(type=AttributeKey.TYPE_DOUBLE, name=name)


def _str_attr(name: str = "sentry.op") -> AttributeKey:
    return AttributeKey(type=AttributeKey.TYPE_STRING, name=name)


def _eq_filter(name: str = "sentry.op", value: str = "http.server") -> TraceItemFilter:
    return TraceItemFilter(
        comparison_filter=ComparisonFilter(
            key=_str_attr(name),
            op=ComparisonFilter.OP_EQUALS,
            value=AttributeValue(val_str=value),
        )
    )


def test_table_samples() -> None:
    request = TraceItemTableRequest(
        meta=_meta(),
        columns=[Column(key=_attr(), label="duration")],
    )
    info = extract_query_info(request)
    assert info["query_type"] == "table_samples"
    assert info["has_aggregate"] == "false"
    assert info["has_groupby"] == "false"
    assert info["trace_item_type"] == "span"
    assert info["filter_profile"] == "none"
    assert info["select_count"] == "1"


def test_table_aggregate_and_groupby() -> None:
    request = TraceItemTableRequest(
        meta=_meta(TraceItemType.TRACE_ITEM_TYPE_LOG),
        columns=[
            Column(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=_attr(),
                    label="avg_duration",
                ),
                label="avg_duration",
            )
        ],
        group_by=[_str_attr()],
        filter=_eq_filter(),
    )
    info = extract_query_info(request)
    assert info["query_type"] == "table_groupby"
    assert info["has_aggregate"] == "true"
    assert info["has_groupby"] == "true"
    assert info["groupby_count"] == "1"
    assert info["trace_item_type"] == "log"
    assert info["filter_profile"] == "equality"
    assert info["filter_leaf_count"] == "1"


def test_table_formula_groupby() -> None:
    left = Column(
        aggregation=AttributeAggregation(aggregate=Function.FUNCTION_SUM, key=_attr(), label="a")
    )
    right = Column(
        aggregation=AttributeAggregation(aggregate=Function.FUNCTION_COUNT, key=_attr(), label="b")
    )
    request = TraceItemTableRequest(
        meta=_meta(),
        columns=[
            Column(
                formula=Column.BinaryFormula(
                    op=Column.BinaryFormula.OP_DIVIDE,
                    left=left,
                    right=right,
                ),
                label="ratio",
            )
        ],
        group_by=[_str_attr("a"), _str_attr("b")],
    )
    info = extract_query_info(request)
    assert info["query_type"] == "table_formula_groupby"
    assert info["has_formula"] == "true"
    assert info["groupby_count"] == "2_3"


def test_table_cross_item_takes_priority() -> None:
    request = TraceItemTableRequest(
        meta=_meta(),
        columns=[
            Column(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT, key=_attr(), label="c"
                ),
                label="c",
            )
        ],
        group_by=[_str_attr()],
        trace_filters=[],
    )
    # Populate cross-item filter list via the repeated field.
    request.trace_filters.add()
    info = extract_query_info(request)
    assert info["query_type"] == "table_cross_item"
    assert info["has_cross_item"] == "true"


def test_timeseries_formula() -> None:
    request = TimeSeriesRequest(
        meta=_meta(),
        granularity_secs=60,
        expressions=[
            TimeSeriesExpression(
                formula=TimeSeriesExpression.BinaryFormula(
                    op=TimeSeriesExpression.BinaryFormula.OP_DIVIDE,
                    left=TimeSeriesExpression(
                        aggregation=AttributeAggregation(
                            aggregate=Function.FUNCTION_SUM, key=_attr(), label="a"
                        ),
                        label="a",
                    ),
                    right=TimeSeriesExpression(
                        aggregation=AttributeAggregation(
                            aggregate=Function.FUNCTION_COUNT, key=_attr(), label="b"
                        ),
                        label="b",
                    ),
                ),
                label="ratio",
            )
        ],
    )
    info = extract_query_info(request)
    assert info["query_type"] == "timeseries_formula"
    assert info["has_formula"] == "true"
    assert info["has_aggregate"] == "true"
    assert info["granularity"] == "lte_1m"


def test_timeseries_groupby() -> None:
    request = TimeSeriesRequest(
        meta=_meta(),
        granularity_secs=3600,
        expressions=[
            TimeSeriesExpression(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG, key=_attr(), label="avg"
                ),
                label="avg",
            )
        ],
        group_by=[_str_attr()],
    )
    info = extract_query_info(request)
    assert info["query_type"] == "timeseries_groupby"
    assert info["granularity"] == "lte_1h"
    assert info["groupby_count"] == "1"


def test_stats_heatmap_and_distributions() -> None:
    heatmap = TraceItemStatsRequest(
        meta=_meta(),
        stats_types=[
            StatsType(
                heatmap=HeatmapRequest(
                    x_attribute=_str_attr(),
                    y_attribute=_attr(),
                    num_y_buckets=10,
                )
            )
        ],
    )
    assert extract_query_info(heatmap)["query_type"] == "stats_heatmap"

    distributions = TraceItemStatsRequest(
        meta=_meta(),
        stats_types=[
            StatsType(attribute_distributions=AttributeDistributionsRequest(max_buckets=10))
        ],
    )
    assert extract_query_info(distributions)["query_type"] == "stats_distributions"

    mixed = TraceItemStatsRequest(
        meta=_meta(),
        stats_types=[
            StatsType(heatmap=HeatmapRequest(x_attribute=_str_attr(), y_attribute=_attr())),
            StatsType(attribute_distributions=AttributeDistributionsRequest()),
        ],
    )
    assert extract_query_info(mixed)["query_type"] == "stats_mixed"


def test_filter_profiles() -> None:
    like_filter = TraceItemFilter(
        comparison_filter=ComparisonFilter(
            key=_str_attr("message"),
            op=ComparisonFilter.OP_LIKE,
            value=AttributeValue(val_str="%error%"),
        )
    )
    info = extract_query_info(TraceItemTableRequest(meta=_meta(), filter=like_filter))
    assert info["filter_profile"] == "like"
    assert info["has_like_filter"] == "true"

    search_filter = TraceItemFilter(
        any_attribute_filter=AnyAttributeFilter(
            op=AnyAttributeFilter.OP_LIKE,
            value=AttributeValue(val_str="%foo%"),
        )
    )
    info = extract_query_info(TraceItemTableRequest(meta=_meta(), filter=search_filter))
    assert info["filter_profile"] == "full_text_search"
    assert info["has_search_filter"] == "true"

    or_filter = TraceItemFilter(
        or_filter=OrFilter(
            filters=[
                _eq_filter("a", "1"),
                _eq_filter("b", "2"),
            ]
        )
    )
    info = extract_query_info(TraceItemTableRequest(meta=_meta(), filter=or_filter))
    assert info["filter_profile"] == "or_equality"
    assert info["has_or_filter"] == "true"
    assert info["filter_leaf_count"] == "2_5"
    assert info["filter_depth"] == "2_5"

    range_filter = TraceItemFilter(
        comparison_filter=ComparisonFilter(
            key=_attr(),
            op=ComparisonFilter.OP_GREATER_THAN,
            value=AttributeValue(val_double=100.0),
        )
    )
    info = extract_query_info(TraceItemTableRequest(meta=_meta(), filter=range_filter))
    assert info["filter_profile"] == "range"

    nested = TraceItemFilter(
        and_filter=AndFilter(
            filters=[
                _eq_filter(),
                TraceItemFilter(exists_filter=ExistsFilter(key=_str_attr("http.status"))),
                range_filter,
            ]
        )
    )
    info = extract_query_info(TraceItemTableRequest(meta=_meta(), filter=nested))
    assert info["filter_profile"] == "mixed"
    assert info["filter_leaf_count"] == "2_5"


def test_metric_tags_are_low_cardinality_subset() -> None:
    request = TraceItemTableRequest(
        meta=_meta(),
        columns=[
            Column(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT, key=_attr(), label="c"
                ),
                label="c",
            )
        ],
        group_by=[_str_attr()],
        filter=_eq_filter(),
    )
    full = extract_query_info(request)
    metric_tags = extract_query_info_tags(request)
    assert set(metric_tags) == {
        "query_type",
        "trace_item_type",
        "has_groupby",
        "groupby_count",
        "has_formula",
        "has_cross_item",
        "filter_profile",
    }
    for key, value in metric_tags.items():
        assert full[key] == value


def test_unknown_endpoint_defaults() -> None:
    # A bare RequestMeta isn't a full request type; pass a message without shape.
    info = extract_query_info(_meta(), endpoint_name="EndpointGetTrace")
    assert info["query_type"] == "get_trace"
    # No nested `.meta` on RequestMeta itself, so item type falls back.
    assert info["trace_item_type"] == "none"

    info = extract_query_info(_meta(), endpoint_name="SomeFutureEndpoint")
    assert info["query_type"] == "other"


def test_groupby_bucket_overflow() -> None:
    request = TraceItemTableRequest(
        meta=_meta(),
        columns=[Column(key=_attr(), label="d")],
        group_by=[_str_attr(f"k{i}") for i in range(5)],
    )
    info = extract_query_info(request)
    assert info["groupby_count"] == "gt_3"
