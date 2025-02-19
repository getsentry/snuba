from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationAndFilter,
    AggregationComparisonFilter,
    AggregationFilter,
    AggregationOrFilter,
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
)

from snuba.web.rpc.v1.endpoint_trace_item_table import (
    convert_to_conditional_aggregation,
)


def _build_sum_attribute_aggregation_column_with_name(name: str) -> Column:
    return Column(
        aggregation=AttributeAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name=name),
            label="sum(" + name + ")",
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
        )
    )


def _build_sum_attribute_conditional_aggregation_column_with_name(name: str) -> Column:
    return Column(
        conditional_aggregation=AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name=name),
            label="sum(" + name + ")",
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
        ),
    )


def _build_avg_aggregation_comparison_filter_with_name(
    name: str,
) -> AggregationComparisonFilter:
    return AggregationComparisonFilter(
        aggregation=AttributeAggregation(
            aggregate=Function.FUNCTION_AVG,
            key=AttributeKey(
                type=AttributeKey.TYPE_DOUBLE,
                name=name,
            ),
        ),
        op=AggregationComparisonFilter.OP_GREATER_THAN,
        val=350,
    )


def _build_avg_conditional_aggregation_comparison_filter_with_name(
    name: str,
) -> AggregationComparisonFilter:
    return AggregationComparisonFilter(
        conditional_aggregation=AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_AVG,
            key=AttributeKey(
                type=AttributeKey.TYPE_DOUBLE,
                name=name,
            ),
        ),
        op=AggregationComparisonFilter.OP_GREATER_THAN,
        val=350,
    )


def _build_unimportant_request_meta() -> RequestMeta:
    return RequestMeta(
        project_ids=[1, 2, 3],
        organization_id=1,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=1),
        end_timestamp=Timestamp(seconds=2),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
    )


def test_convert_aggregation_to_conditional_aggregation_in_select() -> None:
    message = TraceItemTableRequest(
        meta=_build_unimportant_request_meta(),
        columns=[
            Column(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="doesntmatter")
            ),
            _build_sum_attribute_aggregation_column_with_name("column_1"),
            _build_sum_attribute_aggregation_column_with_name("column_2"),
            Column(
                formula=Column.BinaryFormula(
                    op=Column.BinaryFormula.OP_DIVIDE,
                    left=Column(
                        formula=Column.BinaryFormula(
                            op=Column.BinaryFormula.OP_MULTIPLY,
                            left=Column(
                                formula=Column.BinaryFormula(
                                    op=Column.BinaryFormula.OP_ADD,
                                    left=_build_sum_attribute_aggregation_column_with_name(
                                        "add_1"
                                    ),
                                    right=_build_sum_attribute_aggregation_column_with_name(
                                        "add_2"
                                    ),
                                ),
                                label="sum(add_1) + sum(add_2)",
                            ),
                            right=_build_sum_attribute_aggregation_column_with_name(
                                "mult_2"
                            ),
                        ),
                        label="(sum(add_1) + sum(add_2)) * sum(mult_2)",
                    ),
                    right=_build_sum_attribute_aggregation_column_with_name("divide_2"),
                ),
                label="((sum(add_1) + sum(add_2)) * sum(mult_2)) / sum(divide_2)",
            ),
        ],
    )
    convert_to_conditional_aggregation(message)
    assert message.columns == [
        Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="doesntmatter")),
        _build_sum_attribute_conditional_aggregation_column_with_name("column_1"),
        _build_sum_attribute_conditional_aggregation_column_with_name("column_2"),
        Column(
            formula=Column.BinaryFormula(
                op=Column.BinaryFormula.OP_DIVIDE,
                left=Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_MULTIPLY,
                        left=Column(
                            formula=Column.BinaryFormula(
                                op=Column.BinaryFormula.OP_ADD,
                                left=_build_sum_attribute_conditional_aggregation_column_with_name(
                                    "add_1"
                                ),
                                right=_build_sum_attribute_conditional_aggregation_column_with_name(
                                    "add_2"
                                ),
                            ),
                            label="sum(add_1) + sum(add_2)",
                        ),
                        right=_build_sum_attribute_conditional_aggregation_column_with_name(
                            "mult_2"
                        ),
                    ),
                    label="(sum(add_1) + sum(add_2)) * sum(mult_2)",
                ),
                right=_build_sum_attribute_conditional_aggregation_column_with_name(
                    "divide_2"
                ),
            ),
            label="((sum(add_1) + sum(add_2)) * sum(mult_2)) / sum(divide_2)",
        ),
    ]


def test_convert_aggregation_to_conditional_aggregation_in_order_by() -> None:
    # this is not a valid message because none of the columns in `order_by` are in `select`, but it's fine because we're just testing convert_to_conditional_aggregation
    message = TraceItemTableRequest(
        meta=_build_unimportant_request_meta(),
        order_by=[
            TraceItemTableRequest.OrderBy(
                column=_build_sum_attribute_aggregation_column_with_name("column_1")
            ),
            TraceItemTableRequest.OrderBy(
                column=Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=_build_sum_attribute_aggregation_column_with_name(
                            "column_1"
                        ),
                        right=Column(
                            formula=Column.BinaryFormula(
                                op=Column.BinaryFormula.OP_ADD,
                                left=_build_sum_attribute_aggregation_column_with_name(
                                    "column_2"
                                ),
                                right=_build_sum_attribute_aggregation_column_with_name(
                                    "column_3"
                                ),
                            ),
                            label="sum(column_2) + sum(column_3)",
                        ),
                    ),
                    label="sum(column_1) / (sum(column_2) + sum(column_3))",
                )
            ),
        ],
    )
    convert_to_conditional_aggregation(message)
    assert message.order_by == [
        TraceItemTableRequest.OrderBy(
            column=_build_sum_attribute_conditional_aggregation_column_with_name(
                "column_1"
            )
        ),
        TraceItemTableRequest.OrderBy(
            column=Column(
                formula=Column.BinaryFormula(
                    op=Column.BinaryFormula.OP_DIVIDE,
                    left=_build_sum_attribute_conditional_aggregation_column_with_name(
                        "column_1"
                    ),
                    right=Column(
                        formula=Column.BinaryFormula(
                            op=Column.BinaryFormula.OP_ADD,
                            left=_build_sum_attribute_conditional_aggregation_column_with_name(
                                "column_2"
                            ),
                            right=_build_sum_attribute_conditional_aggregation_column_with_name(
                                "column_3"
                            ),
                        ),
                        label="sum(column_2) + sum(column_3)",
                    ),
                ),
                label="sum(column_1) / (sum(column_2) + sum(column_3))",
            )
        ),
    ]


def test_convert_aggregation_to_conditional_aggregation_in_having() -> None:
    message = TraceItemTableRequest(
        meta=_build_unimportant_request_meta(),
        aggregation_filter=AggregationFilter(
            and_filter=AggregationAndFilter(
                filters=[
                    AggregationFilter(
                        comparison_filter=_build_avg_aggregation_comparison_filter_with_name(
                            "column_3"
                        )
                    ),
                    AggregationFilter(
                        and_filter=AggregationAndFilter(
                            filters=[
                                AggregationFilter(
                                    or_filter=AggregationOrFilter(
                                        filters=[
                                            AggregationFilter(
                                                comparison_filter=_build_avg_aggregation_comparison_filter_with_name(
                                                    "column_4"
                                                )
                                            ),
                                            AggregationFilter(
                                                comparison_filter=_build_avg_aggregation_comparison_filter_with_name(
                                                    "column_5"
                                                )
                                            ),
                                        ]
                                    )
                                ),
                                AggregationFilter(
                                    comparison_filter=_build_avg_aggregation_comparison_filter_with_name(
                                        "column_6"
                                    )
                                ),
                            ]
                        )
                    ),
                ]
            )
        ),
    )
    convert_to_conditional_aggregation(message)
    assert (
        message.aggregation_filter
        == AggregationFilter(  # same filter on both sides of the and
            and_filter=AggregationAndFilter(
                filters=[
                    AggregationFilter(
                        comparison_filter=_build_avg_conditional_aggregation_comparison_filter_with_name(
                            "column_3"
                        )
                    ),
                    AggregationFilter(
                        and_filter=AggregationAndFilter(
                            filters=[
                                AggregationFilter(
                                    or_filter=AggregationOrFilter(
                                        filters=[
                                            AggregationFilter(
                                                comparison_filter=_build_avg_conditional_aggregation_comparison_filter_with_name(
                                                    "column_4"
                                                )
                                            ),
                                            AggregationFilter(
                                                comparison_filter=_build_avg_conditional_aggregation_comparison_filter_with_name(
                                                    "column_5"
                                                )
                                            ),
                                        ]
                                    )
                                ),
                                AggregationFilter(
                                    comparison_filter=_build_avg_conditional_aggregation_comparison_filter_with_name(
                                        "column_6"
                                    )
                                ),
                            ]
                        )
                    ),
                ]
            )
        )
    )


def test_convert_aggregation_to_conditional_aggregation_in_all_of_select_and_order_by_and_having() -> None:
    message = TraceItemTableRequest(
        meta=_build_unimportant_request_meta(),
        columns=[
            _build_sum_attribute_aggregation_column_with_name("column_1"),
            _build_sum_attribute_aggregation_column_with_name("column_2"),
        ],
        order_by=[
            TraceItemTableRequest.OrderBy(
                column=_build_sum_attribute_aggregation_column_with_name("column_3")
            ),
            TraceItemTableRequest.OrderBy(
                column=_build_sum_attribute_aggregation_column_with_name("column_4")
            ),
        ],
        aggregation_filter=AggregationFilter(
            comparison_filter=_build_avg_aggregation_comparison_filter_with_name(
                "column_5"
            )
        ),
    )

    convert_to_conditional_aggregation(message)
    assert message.columns == [
        _build_sum_attribute_conditional_aggregation_column_with_name("column_1"),
        _build_sum_attribute_conditional_aggregation_column_with_name("column_2"),
    ]
    assert message.order_by == [
        TraceItemTableRequest.OrderBy(
            column=_build_sum_attribute_conditional_aggregation_column_with_name(
                "column_3"
            )
        ),
        TraceItemTableRequest.OrderBy(
            column=_build_sum_attribute_conditional_aggregation_column_with_name(
                "column_4"
            )
        ),
    ]
    assert message.aggregation_filter == AggregationFilter(
        comparison_filter=_build_avg_conditional_aggregation_comparison_filter_with_name(
            "column_5"
        )
    )
