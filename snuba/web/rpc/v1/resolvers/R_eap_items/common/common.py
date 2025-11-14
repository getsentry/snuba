from collections import defaultdict
from typing import Final, Mapping, Sequence, cast

from sentry_conventions.attributes import ATTRIBUTE_METADATA
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)

from snuba.query import Query
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal, literals_array
from snuba.query.expressions import Expression, FunctionCall, SubscriptableReference
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

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
    AttributeKey.Type.TYPE_BOOLEAN: "attributes_float",
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
    if attribute_type in {AttributeKey.Type.TYPE_INT, AttributeKey.Type.TYPE_BOOLEAN}:
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
    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise BadSnubaRPCRequestException(
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
            raise BadSnubaRPCRequestException(
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

    raise BadSnubaRPCRequestException(
        f"Attribute {attr_key.name} has an unknown type: {AttributeKey.Type.Name(attr_key.type)}"
    )


def apply_virtual_columns(
    query: Query, virtual_column_contexts: Sequence[VirtualColumnContext]
) -> None:
    """Injects virtual column mappings into the clickhouse query. Works with NORMALIZED_COLUMNS on the table or
    dynamic columns in attr_str

    attr_num not supported because mapping on floats is a bad idea

    Example:

        SELECT
          project_name AS `project_name`,
          attr_str['release'] AS `release`,
          attr_str['sentry.sdk.name'] AS `sentry.sdk.name`,
        ... rest of query

        contexts:
            [   {from_column_name: project_id, to_column_name: project_name, value_map: {1: "sentry", 2: "snuba"}} ]


        Query will be transformed into:

        SELECT
        -- see the project name column transformed and the value mapping injected
          transform( CAST( project_id, 'String'), array( '1', '2'), array( 'sentry', 'snuba'), 'unknown') AS `project_name`,
        --
          attr_str['release'] AS `release`,
          attr_str['sentry.sdk.name'] AS `sentry.sdk.name`,
        ... rest of query

    """

    if not virtual_column_contexts:
        return

    mapped_column_to_context = {c.to_column_name: c for c in virtual_column_contexts}

    def transform_expressions(expression: Expression) -> Expression:
        # virtual columns will show up as `attr_str[virtual_column_name]` or `attr_num[virtual_column_name]`
        if not isinstance(expression, SubscriptableReference):
            return expression

        if expression.column.column_name != "attributes_string":
            return expression
        context = mapped_column_to_context.get(str(expression.key.value))
        if context:
            attribute_expression = attribute_key_to_expression(
                AttributeKey(
                    name=context.from_column_name,
                    type=NORMALIZED_COLUMNS_EAP_ITEMS.get(
                        context.from_column_name, [AttributeKey.TYPE_STRING]
                    )[0],
                )
            )
            return f.transform(
                f.CAST(f.ifNull(attribute_expression, literal("")), "String"),
                literals_array(None, [literal(k) for k in context.value_map.keys()]),
                literals_array(None, [literal(v) for v in context.value_map.values()]),
                literal(context.default_value if context.default_value != "" else "unknown"),
                alias=context.to_column_name,
            )

        return expression

    query.transform_expressions(transform_expressions)
