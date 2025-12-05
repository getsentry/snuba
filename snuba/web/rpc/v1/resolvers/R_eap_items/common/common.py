from typing import Sequence

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)

from snuba.protos.common import (
    ATTRIBUTES_TO_COALESCE,
    COLUMN_PREFIX,
    NORMALIZED_COLUMNS_EAP_ITEMS,
    PROTO_TYPE_TO_ATTRIBUTE_COLUMN,
    PROTO_TYPE_TO_CLICKHOUSE_TYPE,
    MalformedAttributeException,
)
from snuba.protos.common import (
    attribute_key_to_expression as _attribute_key_to_expression,
)
from snuba.query import Query
from snuba.query.dsl import Functions as f
from snuba.query.dsl import literal, literals_array
from snuba.query.expressions import Expression, SubscriptableReference
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

# Re-export for backward compatibility
__all__ = [
    "ATTRIBUTES_TO_COALESCE",
    "COLUMN_PREFIX",
    "NORMALIZED_COLUMNS_EAP_ITEMS",
    "PROTO_TYPE_TO_ATTRIBUTE_COLUMN",
    "PROTO_TYPE_TO_CLICKHOUSE_TYPE",
    "attribute_key_to_expression",
    "apply_virtual_columns",
]


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    """Convert an AttributeKey proto to a Snuba Expression.

    This is a wrapper around the proto-layer function that converts
    MalformedAttributeException to BadSnubaRPCRequestException for
    HTTP-aware code paths.

    Raises:
        BadSnubaRPCRequestException: If the attribute key is invalid or malformed.
    """
    try:
        return _attribute_key_to_expression(attr_key)
    except MalformedAttributeException as e:
        raise BadSnubaRPCRequestException(str(e)) from e


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
