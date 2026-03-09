from collections import defaultdict
from typing import Final, Mapping, Sequence, cast

from sentry_conventions.attributes import ATTRIBUTE_METADATA
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import arrayElement, column, literal
from snuba.query.expressions import (
    Argument,
    Expression,
    FunctionCall,
    JsonPath,
    Lambda,
    SubscriptableReference,
)


class MalformedAttributeException(Exception):
    """Exception raised when an AttributeKey proto is malformed or invalid.

    This exception is HTTP-agnostic and should be caught and wrapped by
    HTTP-aware code paths that need to return appropriate status codes.
    """

    pass


COLUMN_PREFIX: str = "sentry."

NORMALIZED_COLUMNS_EAP_ITEMS: Final[Mapping[str, Sequence[AttributeKey.Type.ValueType]]] = {
    f"{COLUMN_PREFIX}organization_id": [AttributeKey.Type.TYPE_INT],
    f"{COLUMN_PREFIX}project_id": [AttributeKey.Type.TYPE_INT],
    f"{COLUMN_PREFIX}timestamp": [
        AttributeKey.Type.TYPE_FLOAT,
        AttributeKey.Type.TYPE_DOUBLE,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_STRING,
    ],
    f"{COLUMN_PREFIX}trace_id": [
        AttributeKey.Type.TYPE_STRING
    ],  # this gets converted from a uuid to a string in a storage processor
    f"{COLUMN_PREFIX}item_id": [AttributeKey.Type.TYPE_STRING],
    f"{COLUMN_PREFIX}sampling_weight": [AttributeKey.Type.TYPE_DOUBLE],
    f"{COLUMN_PREFIX}sampling_factor": [AttributeKey.Type.TYPE_DOUBLE],
}

PROTO_TYPE_TO_CLICKHOUSE_TYPE: Final[Mapping[AttributeKey.Type.ValueType, str]] = {
    AttributeKey.Type.TYPE_INT: "Int64",
    AttributeKey.Type.TYPE_STRING: "String",
    AttributeKey.Type.TYPE_DOUBLE: "Float64",
    AttributeKey.Type.TYPE_FLOAT: "Float64",
    AttributeKey.Type.TYPE_BOOLEAN: "Boolean",
}

PROTO_TYPE_TO_ATTRIBUTE_COLUMN: Final[Mapping[AttributeKey.Type.ValueType, str]] = {
    AttributeKey.Type.TYPE_INT: "attributes_float",
    AttributeKey.Type.TYPE_STRING: "attributes_string",
    AttributeKey.Type.TYPE_DOUBLE: "attributes_float",
    AttributeKey.Type.TYPE_FLOAT: "attributes_float",
    AttributeKey.Type.TYPE_BOOLEAN: "attributes_bool",
}


def _build_deprecated_attributes() -> dict[str, set[str]]:
    current_to_deprecated: dict[str, set[str]] = defaultdict(set)
    for name, metadata in ATTRIBUTE_METADATA.items():
        if metadata.deprecation:
            replacement = cast(str, metadata.deprecation.replacement)
            deprecated = {name}
            if metadata.aliases:
                deprecated.update(metadata.aliases)
            current_to_deprecated[replacement].update(deprecated)
    return current_to_deprecated


ATTRIBUTES_TO_COALESCE: dict[str, set[str]] = _build_deprecated_attributes()


def _build_label_mapping_key(attribute_key: AttributeKey) -> str:
    return f"{attribute_key.name}_{AttributeKey.Type.Name(attribute_key.type)}"


def _generate_subscriptable_reference(
    attribute_name: str,
    attribute_type: AttributeKey.Type.ValueType,
    alias: str | None = None,
) -> SubscriptableReference | FunctionCall:
    kwargs = {}
    if alias:
        kwargs["alias"] = alias
    clickhouse_type = PROTO_TYPE_TO_CLICKHOUSE_TYPE[attribute_type]
    if attribute_type == AttributeKey.Type.TYPE_BOOLEAN:
        # Boolean attributes use a Map column without hash buckets,
        # so we use arrayElement directly instead of SubscriptableReference
        # which would be transformed to the nested .key/.value pattern.
        return f.cast(
            arrayElement(
                None,
                column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attribute_type]),
                literal(attribute_name),
            ),
            f"Nullable({clickhouse_type})",
            **kwargs,
        )
    if attribute_type == AttributeKey.Type.TYPE_INT:
        return f.cast(
            SubscriptableReference(
                column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attribute_type]),
                key=literal(attribute_name),
                alias=None,
            ),
            f"Nullable({clickhouse_type})",
            **kwargs,
        )
    return SubscriptableReference(
        column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attribute_type]),
        key=literal(attribute_name),
        alias=alias,
    )


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    """Convert an AttributeKey proto to a Snuba Expression.

    Raises:
        MalformedAttributeException: If the attribute key is invalid or malformed.
    """
    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise MalformedAttributeException(
            f"attribute key {attr_key.name} must have a type specified"
        )

    alias = _build_label_mapping_key(attr_key)

    if attr_key.name == "attr_key":
        return column("attr_key")

    if attr_key.name in NORMALIZED_COLUMNS_EAP_ITEMS:
        if attr_key.type not in NORMALIZED_COLUMNS_EAP_ITEMS[attr_key.name]:
            formatted_attribute_types = ", ".join(
                map(
                    AttributeKey.Type.Name,
                    NORMALIZED_COLUMNS_EAP_ITEMS[attr_key.name],
                )
            )
            raise MalformedAttributeException(
                f"Attribute {attr_key.name} must be one of [{formatted_attribute_types}], got {AttributeKey.Type.Name(attr_key.type)}"
            )

        return f.cast(
            column(attr_key.name[len(COLUMN_PREFIX) :]),
            PROTO_TYPE_TO_CLICKHOUSE_TYPE[attr_key.type],
            alias=alias,
        )

    if attr_key.type in PROTO_TYPE_TO_ATTRIBUTE_COLUMN:
        if attr_key.name in ATTRIBUTES_TO_COALESCE:
            expressions = [
                _generate_subscriptable_reference(
                    attribute_name,
                    attr_key.type,
                )
                for attribute_name in [
                    attr_key.name,
                ]
                + list(ATTRIBUTES_TO_COALESCE[attr_key.name])
            ]
            return f.coalesce(
                *expressions,
                alias=alias,
            )
        else:
            return _generate_subscriptable_reference(
                attr_key.name,
                attr_key.type,
                alias,
            )

    if attr_key.type == AttributeKey.Type.TYPE_ARRAY:
        alias = _build_label_mapping_key(attr_key)
        # Array values are stored as tagged variants (e.g. {"String": "alice"},
        # {"Int": "123"}) in the JSON column. Cast to Array(JSON), then extract
        # the value from whichever variant tag is present. We coalesce across
        # all supported types, converting non-string types via toString so the
        # result is always a string array.
        x = Argument(None, "x")
        return FunctionCall(
            alias=alias,
            function_name="arrayMap",
            parameters=(
                Lambda(
                    alias=None,
                    parameters=("x",),
                    transformation=FunctionCall(
                        alias=None,
                        function_name="coalesce",
                        parameters=(
                            JsonPath(None, x, "String", "Nullable(String)"),
                            FunctionCall(
                                None,
                                "toString",
                                (JsonPath(None, x, "Int", "Nullable(Int64)"),),
                            ),
                            FunctionCall(
                                None,
                                "toString",
                                (JsonPath(None, x, "Double", "Nullable(Float64)"),),
                            ),
                            JsonPath(None, x, "Bool", "Nullable(String)"),
                        ),
                    ),
                ),
                JsonPath(
                    alias=None,
                    base=column("attributes_array"),
                    path=attr_key.name,
                    return_type="Array(JSON)",
                ),
            ),
        )

    raise MalformedAttributeException(
        f"Attribute {attr_key.name} has an unknown type: {AttributeKey.Type.Name(attr_key.type)}"
    )
