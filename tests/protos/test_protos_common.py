import pytest
from sentry_conventions.attributes import ATTRIBUTE_METADATA
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.protos.common import (
    ATTRIBUTES_TO_COALESCE,
    MalformedAttributeException,
    _resolve_canonical,
    attribute_key_to_expression,
)
from snuba.query.dsl import Functions as f
from snuba.query.dsl import arrayElement, column, literal, map_key_exists
from snuba.query.expressions import Expression, FunctionCall, SubscriptableReference


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
        names = [new_attribute, *old_attributes]

        def sub(name: str) -> SubscriptableReference:
            return SubscriptableReference(
                alias=None,
                column=column("attributes_string"),
                key=literal(name),
            )

        multiif_args: list[Expression] = []
        for name in names[:-1]:
            multiif_args.append(map_key_exists(column("attributes_string"), literal(name)))
            multiif_args.append(sub(name))
        multiif_args.append(sub(names[-1]))

        assert attribute_key_to_expression(
            AttributeKey(
                type=AttributeKey.TYPE_STRING,
                name=new_attribute,
            ),
        ) == f.multiIf(*multiif_args, alias=f"{new_attribute}_TYPE_STRING")

    def test_coalesce_queried_attribute_is_first(self) -> None:
        for name in ATTRIBUTES_TO_COALESCE:
            result = attribute_key_to_expression(
                AttributeKey(type=AttributeKey.TYPE_STRING, name=name),
            )
            assert isinstance(result, FunctionCall)
            assert result.function_name == "multiIf"
            value_params = list(result.parameters[1::2]) + [result.parameters[-1]]
            first_param = value_params[0]
            assert isinstance(first_param, SubscriptableReference)
            assert first_param.key.value == name, (
                f"Expected {name} as first coalesced value, got {first_param.key.value}"
            )
            remaining: list[str] = [
                str(p.key.value) for p in value_params[1:] if isinstance(p, SubscriptableReference)
            ]
            meta = ATTRIBUTE_METADATA.get(name)
            is_deprecated = (
                meta is not None
                and meta.deprecation is not None
                and meta.deprecation.replacement is not None
            )
            if is_deprecated:
                canonical = _resolve_canonical(name)
                assert remaining[0] == canonical, (
                    f"Expected canonical {canonical} as second coalesced value for deprecated {name}, got {remaining[0]}"
                )
                assert remaining[1:] == sorted(remaining[1:]), (
                    f"Coalesced values after canonical for {name} are not sorted: {remaining[1:]}"
                )
            else:
                assert remaining == sorted(remaining), (
                    f"Coalesced values after {name} are not sorted: {remaining}"
                )

    def test_coalesce_bidirectional(self) -> None:
        for name, others in ATTRIBUTES_TO_COALESCE.items():
            for other in others:
                assert other in ATTRIBUTES_TO_COALESCE, (
                    f"{other} (in group with {name}) is not a key in ATTRIBUTES_TO_COALESCE"
                )
                assert name in ATTRIBUTES_TO_COALESCE[other], (
                    f"{name} not in ATTRIBUTES_TO_COALESCE[{other}]"
                )

    def test_coalesce_map_does_not_include_none_key(self) -> None:
        assert None not in ATTRIBUTES_TO_COALESCE

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
