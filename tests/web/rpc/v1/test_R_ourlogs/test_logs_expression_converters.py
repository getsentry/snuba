from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    IntArray,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    ExistsFilter,
    TraceItemFilter,
)

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import FunctionCall
from snuba.web.rpc.common.common import trace_item_filters_to_expression
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.attribute_key_to_expression import (
    attribute_key_to_expression,
)


class TestOurlogsExpressionConverters:
    def test_expression_trace_id(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="sentry.trace_id",
            ),
        ) == f.CAST(column("trace_id"), "String", alias="sentry.trace_id_TYPE_STRING")

    def test_timestamp_column(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="sentry.timestamp",
            ),
        ) == f.CAST(column("timestamp"), "String", alias="sentry.timestamp_TYPE_STRING")
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_INT,
                name="sentry.timestamp",
            ),
        ) == f.CAST(column("timestamp"), "Int64", alias="sentry.timestamp_TYPE_INT")
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_FLOAT,
                name="sentry.timestamp",
            ),
        ) == f.CAST(column("timestamp"), "Float64", alias="sentry.timestamp_TYPE_FLOAT")

    def test_normalized_col(self) -> None:
        for col in [
            "sentry.span_id",
            "sentry.severity_text",
            "sentry.body",
        ]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == column(col[len("sentry.") :], alias=col)

    def test_attr_buckets(self) -> None:
        for (typ, col) in [
            (AttributeKey.TYPE_STRING, "attr_string"),
            (AttributeKey.TYPE_FLOAT, "attr_double"),
            (AttributeKey.TYPE_INT, "attr_int"),
            (AttributeKey.TYPE_BOOLEAN, "attr_bool"),
        ]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=typ,
                    name="z",
                ),
            ) == FunctionCall(
                f"z_{AttributeKey.Type.Name(typ)}",
                "arrayElement",
                (column(col), literal("z")),
            )

    def test_trace_item_filters_to_expression(self) -> None:
        assert trace_item_filters_to_expression(
            TraceItemFilter(
                and_filter=AndFilter(
                    filters=[
                        TraceItemFilter(
                            exists_filter=ExistsFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="hello"
                                )
                            )
                        ),
                        TraceItemFilter(
                            exists_filter=ExistsFilter(
                                key=AttributeKey(type=AttributeKey.TYPE_INT, name="two")
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="world"
                                ),
                                op=ComparisonFilter.OP_IN,
                                value=AttributeValue(
                                    val_int_array=IntArray(values=[1, 2, 3])
                                ),
                            )
                        ),
                    ]
                )
            ),
            attribute_key_to_expression,
        ) == FunctionCall(
            None,
            "and",
            (
                f.mapContains(column("attr_string"), literal("hello")),
                f.mapContains(column("attr_int"), literal("two")),
                FunctionCall(
                    None,
                    "in",
                    (
                        f.arrayElement(
                            column("attr_int"), literal("world"), alias="world_TYPE_INT"
                        ),
                        f.array(literal(1), literal(2), literal(3)),
                    ),
                ),
            ),
        )
