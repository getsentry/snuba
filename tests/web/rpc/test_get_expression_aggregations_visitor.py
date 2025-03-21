from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import Expression
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    Function,
)

from snuba.web.rpc.proto_visitor import (
    GetExpressionAggregationsVisitor,
    TimeSeriesExpressionWrapper,
)


def test_formula() -> None:
    expression = Expression(
        formula=Expression.BinaryFormula(
            op=Expression.BinaryFormula.OP_DIVIDE,
            left=Expression(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric1"),
                    label="sum",
                )
            ),
            right=Expression(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric2"),
                    label="sum",
                )
            ),
        )
    )
    vis = GetExpressionAggregationsVisitor()
    TimeSeriesExpressionWrapper(expression).accept(vis)
    assert vis.aggregations == [
        AttributeAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric1"),
            label="sum",
        ),
        AttributeAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric2"),
            label="sum",
        ),
    ]


def test_basic() -> None:
    expression = Expression(
        aggregation=AttributeAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric1"),
            label="sum",
        )
    )
    vis = GetExpressionAggregationsVisitor()
    TimeSeriesExpressionWrapper(expression).accept(vis)
    assert vis.aggregations == [
        AttributeAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric1"),
            label="sum",
        )
    ]


def test_conditional_aggregation() -> None:
    expression = Expression(
        conditional_aggregation=AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric1"),
            label="sum",
        )
    )
    vis = GetExpressionAggregationsVisitor()
    TimeSeriesExpressionWrapper(expression).accept(vis)
    assert vis.aggregations == [
        AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric1"),
            label="sum",
        )
    ]
