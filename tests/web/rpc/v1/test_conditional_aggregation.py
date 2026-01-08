from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression,
    TimeSeriesRequest,
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

from snuba.web.rpc.proto_visitor import (
    AggregationToConditionalAggregationVisitor,
    TimeSeriesRequestWrapper,
    TraceItemTableRequestWrapper,
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


_UNIMPORTANT_REQUEST_META = RequestMeta(
    project_ids=[1, 2, 3],
    organization_id=1,
    cogs_category="something",
    referrer="something",
    start_timestamp=Timestamp(seconds=1),
    end_timestamp=Timestamp(seconds=2),
    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
)


class TestEndpointTraceItemTableRequest:
    def test_convert_aggregation_to_conditional_aggregation_in_select(self) -> None:
        message = TraceItemTableRequest(
            meta=_UNIMPORTANT_REQUEST_META,
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="doesntmatter")),
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
                                right=_build_sum_attribute_aggregation_column_with_name("mult_2"),
                            ),
                            label="(sum(add_1) + sum(add_2)) * sum(mult_2)",
                        ),
                        right=_build_sum_attribute_aggregation_column_with_name("divide_2"),
                    ),
                    label="((sum(add_1) + sum(add_2)) * sum(mult_2)) / sum(divide_2)",
                ),
            ],
        )
        aggregation_to_conditional_aggregation_visitor = (
            AggregationToConditionalAggregationVisitor()
        )
        message_wrapper = TraceItemTableRequestWrapper(message)
        message_wrapper.accept(aggregation_to_conditional_aggregation_visitor)

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
                    right=_build_sum_attribute_conditional_aggregation_column_with_name("divide_2"),
                ),
                label="((sum(add_1) + sum(add_2)) * sum(mult_2)) / sum(divide_2)",
            ),
        ]

    def test_convert_aggregation_to_conditional_aggregation_in_order_by(self) -> None:
        # this is not a valid message because none of the columns in `order_by` are in `select`, but it's fine because we're just testing convert_to_conditional_aggregation
        message = TraceItemTableRequest(
            meta=_UNIMPORTANT_REQUEST_META,
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=_build_sum_attribute_aggregation_column_with_name("column_1")
                ),
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        formula=Column.BinaryFormula(
                            op=Column.BinaryFormula.OP_DIVIDE,
                            left=_build_sum_attribute_aggregation_column_with_name("column_1"),
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
        aggregation_to_conditional_aggregation_visitor = (
            AggregationToConditionalAggregationVisitor()
        )
        message_wrapper = TraceItemTableRequestWrapper(message)
        message_wrapper.accept(aggregation_to_conditional_aggregation_visitor)

        assert message.order_by == [
            TraceItemTableRequest.OrderBy(
                column=_build_sum_attribute_conditional_aggregation_column_with_name("column_1")
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

    def test_convert_aggregation_to_conditional_aggregation_in_having(self) -> None:
        message = TraceItemTableRequest(
            meta=_UNIMPORTANT_REQUEST_META,
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
        aggregation_to_conditional_aggregation_visitor = (
            AggregationToConditionalAggregationVisitor()
        )
        message_wrapper = TraceItemTableRequestWrapper(message)
        message_wrapper.accept(aggregation_to_conditional_aggregation_visitor)

        assert message.aggregation_filter == AggregationFilter(  # same filter on both sides of the and
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

    def test_convert_aggregation_to_conditional_aggregation_in_all_of_select_and_order_by_and_having(
        self,
    ) -> None:
        message = TraceItemTableRequest(
            meta=_UNIMPORTANT_REQUEST_META,
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
                comparison_filter=_build_avg_aggregation_comparison_filter_with_name("column_5")
            ),
        )
        aggregation_to_conditional_aggregation_visitor = (
            AggregationToConditionalAggregationVisitor()
        )
        message_wrapper = TraceItemTableRequestWrapper(message)
        message_wrapper.accept(aggregation_to_conditional_aggregation_visitor)

        assert message.columns == [
            _build_sum_attribute_conditional_aggregation_column_with_name("column_1"),
            _build_sum_attribute_conditional_aggregation_column_with_name("column_2"),
        ]
        assert message.order_by == [
            TraceItemTableRequest.OrderBy(
                column=_build_sum_attribute_conditional_aggregation_column_with_name("column_3")
            ),
            TraceItemTableRequest.OrderBy(
                column=_build_sum_attribute_conditional_aggregation_column_with_name("column_4")
            ),
        ]
        assert message.aggregation_filter == AggregationFilter(
            comparison_filter=_build_avg_conditional_aggregation_comparison_filter_with_name(
                "column_5"
            )
        )


def _build_sum_attribute_aggregation_with_label(label: str) -> AttributeAggregation:
    return AttributeAggregation(
        aggregate=Function.FUNCTION_SUM,
        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name=label),
        label="sum(" + label + ")",
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    )


def _build_sum_attribute_conditional_aggregation_with_label(
    label: str,
) -> AttributeConditionalAggregation:
    return AttributeConditionalAggregation(
        aggregate=Function.FUNCTION_SUM,
        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name=label),
        label="sum(" + label + ")",
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    )


class TestTimeSeriesRequest:
    def test_convert_aggregation_to_conditional_aggregation_in_expression(self) -> None:
        message = TimeSeriesRequest(
            meta=_UNIMPORTANT_REQUEST_META,
            expressions=[
                Expression(
                    aggregation=_build_sum_attribute_aggregation_with_label("doenstmatter1")
                ),
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_DIVIDE,
                        left=Expression(
                            formula=Expression.BinaryFormula(
                                op=Expression.BinaryFormula.OP_DIVIDE,
                                left=Expression(
                                    aggregation=_build_sum_attribute_aggregation_with_label(
                                        "doenstmatter2"
                                    )
                                ),
                                right=Expression(
                                    aggregation=_build_sum_attribute_aggregation_with_label(
                                        "doenstmatter3"
                                    )
                                ),
                            ),
                            label="sum(doesntmatter2) / sum(doesntmatter3)",
                        ),
                        right=Expression(
                            formula=Expression.BinaryFormula(
                                op=Expression.BinaryFormula.OP_DIVIDE,
                                left=Expression(
                                    aggregation=_build_sum_attribute_aggregation_with_label(
                                        "doenstmatter4"
                                    )
                                ),
                                right=Expression(
                                    aggregation=_build_sum_attribute_aggregation_with_label(
                                        "doenstmatter5"
                                    )
                                ),
                            ),
                            label="sum(doesntmatter4) / sum(doesntmatter5)",
                        ),
                    ),
                    label="(sum(doesntmatter2) / sum(doesntmatter3)) / (sum(doesntmatter4) / sum(doesntmatter5))",
                ),
            ],
        )
        aggregation_to_conditional_aggregation_visitor = (
            AggregationToConditionalAggregationVisitor()
        )
        message_wrapper = TimeSeriesRequestWrapper(message)
        message_wrapper.accept(aggregation_to_conditional_aggregation_visitor)

        assert message.expressions == [
            Expression(
                conditional_aggregation=_build_sum_attribute_conditional_aggregation_with_label(
                    "doenstmatter1"
                )
            ),
            Expression(
                formula=Expression.BinaryFormula(
                    op=Expression.BinaryFormula.OP_DIVIDE,
                    left=Expression(
                        formula=Expression.BinaryFormula(
                            op=Expression.BinaryFormula.OP_DIVIDE,
                            left=Expression(
                                conditional_aggregation=_build_sum_attribute_conditional_aggregation_with_label(
                                    "doenstmatter2"
                                )
                            ),
                            right=Expression(
                                conditional_aggregation=_build_sum_attribute_conditional_aggregation_with_label(
                                    "doenstmatter3"
                                )
                            ),
                        ),
                        label="sum(doesntmatter2) / sum(doesntmatter3)",
                    ),
                    right=Expression(
                        formula=Expression.BinaryFormula(
                            op=Expression.BinaryFormula.OP_DIVIDE,
                            left=Expression(
                                conditional_aggregation=_build_sum_attribute_conditional_aggregation_with_label(
                                    "doenstmatter4"
                                )
                            ),
                            right=Expression(
                                conditional_aggregation=_build_sum_attribute_conditional_aggregation_with_label(
                                    "doenstmatter5"
                                )
                            ),
                        ),
                        label="sum(doesntmatter4) / sum(doesntmatter5)",
                    ),
                ),
                label="(sum(doesntmatter2) / sum(doesntmatter3)) / (sum(doesntmatter4) / sum(doesntmatter5))",
            ),
        ]
