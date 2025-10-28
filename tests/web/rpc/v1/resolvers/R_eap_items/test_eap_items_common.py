from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import SubscriptableReference
from snuba.web.rpc.v1.resolvers.R_eap_items.common.common import (
    ATTRIBUTES_TO_COALESCE,
    attribute_key_to_expression,
)


class TestCommon:
    def test_expression_trace_id(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="sentry.trace_id",
            ),
        ) == f.cast(column("trace_id"), "String", alias="sentry_trace_id_TYPE_STRING")

    def test_attributes(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_STRING, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_STRING",
            column=column("attributes_string"),
            key=literal("derp"),
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_FLOAT, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_FLOAT",
            column=column("attributes_float"),
            key=literal("derp"),
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="derp"),
        ) == SubscriptableReference(
            alias="derp_TYPE_DOUBLE",
            column=column("attributes_float"),
            key=literal("derp"),
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_INT, name="derp"),
        ) == f.cast(
            SubscriptableReference(
                alias=None,
                column=column("attributes_float"),
                key=literal("derp"),
            ),
            "Nullable(Int64)",
            alias="derp_TYPE_INT",
        )

        assert attribute_key_to_expression(
            AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="derp"),
        ) == f.cast(
            SubscriptableReference(
                alias=None,
                column=column("attributes_float"),
                key=literal("derp"),
            ),
            "Nullable(Boolean)",
            alias="derp_TYPE_BOOLEAN",
        )

    def test_coalesce(self) -> None:
        new_attribute = list(ATTRIBUTES_TO_COALESCE.keys())[0]
        old_attribute = ATTRIBUTES_TO_COALESCE[new_attribute][0]

        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name=new_attribute,
            ),
        ) == f.coalesce(
            SubscriptableReference(
                alias=None,
                column=column("attributes_string"),
                key=literal(new_attribute),
            ),
            SubscriptableReference(
                alias=None,
                column=column("attributes_string"),
                key=literal(old_attribute),
            ),
            alias=f"{new_attribute.replace('.', '_')}_TYPE_STRING",
        )
