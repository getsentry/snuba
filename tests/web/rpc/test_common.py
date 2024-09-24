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
                name="sentry.trace_id",
            ),
        ) == f.CAST(column("trace_id"), "String", alias="sentry.trace_id")

    def test_timestamp_columns(self) -> None:
        for col in [
            "sentry.timestamp",
            "sentry.start_timestamp",
            "sentry.end_timestamp",
        ]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == f.CAST(column(col[len("sentry.") :]), "String", alias=col)
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_INT,
                    name=col,
                ),
            ) == f.CAST(column(col[len("sentry.") :]), "Int64", alias=col)
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_FLOAT,
                    name=col,
                ),
            ) == f.CAST(column(col[len("sentry.") :]), "Float64", alias=col)

    def test_normalized_col(self) -> None:
        for col in [
            "sentry.span_id",
            "sentry.parent_span_id",
            "sentry.segment_id",
            "sentry.service",
        ]:
            assert attribute_key_to_expression(
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name=col,
                ),
            ) == column(col[len("sentry.") :], alias=col)

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
