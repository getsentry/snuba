from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import SubscriptableReference
from snuba.web.rpc.common import attribute_key_to_expression


class TestCommon:
    def test_expression_trace_id(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="trace_id",
            ),
        ) == f.CAST(column("trace_id"), "String", alias="trace_id")

    def test_hex_id_columns(self) -> None:
        for col in ["span_id", "parent_span_id", "segment_id"]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == column(col)

    def test_timestamp_columns(self) -> None:
        for col in ["timestamp", "start_timestamp", "end_timestamp"]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == f.CAST(column(col), "String", alias=col)
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_INT,
                    name=col,
                ),
            ) == f.CAST(column(col), "Int64", alias=col)
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_FLOAT,
                    name=col,
                ),
            ) == f.CAST(column(col), "Float64", alias=col)

    def test_normalized_col(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="service",
            ),
        ) == column("service")

    def test_attributes(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_STRING, name="derp"),
        ) == SubscriptableReference(
            alias="derp", column=column("attr_str"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_FLOAT, name="derp"),
        ) == SubscriptableReference(
            alias="derp", column=column("attr_num"), key=literal("derp")
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_INT, name="derp"),
        ) == f.CAST(
            SubscriptableReference(
                alias="derp",
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Int64",
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="derp"),
        ) == f.CAST(
            SubscriptableReference(
                alias="derp",
                column=column("attr_num"),
                key=literal("derp"),
            ),
            "Boolean",
        )
