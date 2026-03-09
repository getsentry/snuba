import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.protos.common import (
    ATTRIBUTES_TO_COALESCE,
    MalformedAttributeException,
    attribute_key_to_expression,
)
from snuba.query.dsl import Functions as f
from snuba.query.dsl import arrayElement, column, literal
from snuba.query.expressions import SubscriptableReference


class TestAttributeKeyToExpression:
    def test_expression_trace_id(self) -> None:
        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name="sentry.trace_id",
            ),
        ) == f.cast(column("trace_id"), "String", alias="sentry.trace_id_TYPE_STRING")

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
            arrayElement(
                None,
                column("attributes_bool"),
                literal("derp"),
            ),
            "Nullable(Boolean)",
            alias="derp_TYPE_BOOLEAN",
        )

    def test_coalesce(self) -> None:
        new_attribute = list(ATTRIBUTES_TO_COALESCE.keys())[0]
        old_attributes = ATTRIBUTES_TO_COALESCE[new_attribute]
        references = [
            SubscriptableReference(
                alias=None,
                column=column("attributes_string"),
                key=literal(old_attribute),
            )
            for old_attribute in old_attributes
        ]

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
            *references,
            alias=f"{new_attribute}_TYPE_STRING",
        )

    def test_unspecified_type_raises_exception(self) -> None:
        with pytest.raises(MalformedAttributeException) as exc_info:
            attribute_key_to_expression(
                AttributeKey(type=AttributeKey.TYPE_UNSPECIFIED, name="test_attr")
            )
        assert "must have a type specified" in str(exc_info.value)

    def test_invalid_type_for_normalized_column_raises_exception(self) -> None:
        with pytest.raises(MalformedAttributeException) as exc_info:
            attribute_key_to_expression(
                AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="sentry.trace_id")
            )
        assert "must be one of" in str(exc_info.value)
