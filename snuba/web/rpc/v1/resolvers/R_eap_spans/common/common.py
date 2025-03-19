from typing import Final, Mapping, Sequence, Set

from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)

from snuba import settings, state
from snuba.query import Query
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal, literals_array
from snuba.query.expressions import Expression, SubscriptableReference
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

# These are the columns which aren't stored in attr_str_ nor attr_num_ in clickhouse
NORMALIZED_COLUMNS: Final[Mapping[str, AttributeKey.Type.ValueType]] = {
    "sentry.organization_id": AttributeKey.Type.TYPE_INT,
    "sentry.project_id": AttributeKey.Type.TYPE_INT,
    "sentry.service": AttributeKey.Type.TYPE_STRING,
    "sentry.span_id": AttributeKey.Type.TYPE_STRING,  # this is converted by a processor on the storage
    "sentry.parent_span_id": AttributeKey.Type.TYPE_STRING,  # this is converted by a processor on the storage
    "sentry.segment_id": AttributeKey.Type.TYPE_STRING,  # this is converted by a processor on the storage
    "sentry.segment_name": AttributeKey.Type.TYPE_STRING,
    "sentry.is_segment": AttributeKey.Type.TYPE_BOOLEAN,
    "sentry.duration_ms": AttributeKey.Type.TYPE_DOUBLE,
    "sentry.exclusive_time_ms": AttributeKey.Type.TYPE_DOUBLE,
    "sentry.retention_days": AttributeKey.Type.TYPE_INT,
    "sentry.name": AttributeKey.Type.TYPE_STRING,
    "sentry.sampling_weight": AttributeKey.Type.TYPE_DOUBLE,
    "sentry.sampling_factor": AttributeKey.Type.TYPE_DOUBLE,
    "sentry.timestamp": AttributeKey.Type.TYPE_UNSPECIFIED,
    "sentry.start_timestamp": AttributeKey.Type.TYPE_UNSPECIFIED,
    "sentry.end_timestamp": AttributeKey.Type.TYPE_UNSPECIFIED,
}

TIMESTAMP_COLUMNS: Final[Set[str]] = {
    "sentry.timestamp",
    "sentry.start_timestamp",
    "sentry.end_timestamp",
}

# These are attributes that were not stored in attr_str_ or attr_num_ in eap_spans because they were stored in columns.
# Since we store these in the attribute columns in eap_items, we need to exclude them in endpoints that don't expect them to be in the attribute columns.
ATTRIBUTES_TO_EXCLUDE_IN_EAP_ITEMS: Final[Set[str]] = {
    "sentry.raw_description",
    "sentry.transaction",
    "sentry.start_timestamp_precise",
    "sentry.end_timestamp_precise",
    "sentry.duration_ms",
    "sentry.event_id",
    "sentry.exclusive_time_ms",
    "sentry.is_segment",
    "sentry.parent_span_id",
    "sentry.profile_id",
    "sentry.received",
    "sentry.segment_id",
}

COLUMN_PREFIX: str = "sentry."

NORMALIZED_COLUMNS_EAP_ITEMS: Final[
    Mapping[str, Sequence[AttributeKey.Type.ValueType]]
] = {
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

# We have renamed some attributes in eap_items, so to avoid breaking changes we need to map the old names to the new names
ATTRIBUTE_MAPPINGS: Final[Mapping[str, str]] = {
    "sentry.name": "sentry.raw_description",
    "sentry.description": "sentry.normalized_description",
    "sentry.span_id": "sentry.item_id",
    "sentry.segment_name": "sentry.transaction",
    "sentry.start_timestamp": "sentry.start_timestamp_precise",
    "sentry.end_timestamp": "sentry.end_timestamp_precise",
}


def use_eap_items_table(request_meta: RequestMeta) -> bool:
    if request_meta.referrer.startswith("force_use_eap_spans_table"):
        return False

    if settings.USE_EAP_ITEMS_TABLE:
        return True

    use_eap_items_orgs = state.get_config("use_eap_items_orgs")
    if use_eap_items_orgs:
        use_eap_items_orgs = map(int, use_eap_items_orgs.strip("[]").split(","))

    use_eap_items_table_start_timestamp_seconds = state.get_int_config(
        "use_eap_items_table_start_timestamp_seconds"
    )

    if (
        state.get_config("use_eap_items_table", False)
        and use_eap_items_table_start_timestamp_seconds is not None
    ):
        if (
            use_eap_items_orgs
            and request_meta.organization_id not in use_eap_items_orgs
        ):
            return False

        return (
            request_meta.start_timestamp.seconds
            >= use_eap_items_table_start_timestamp_seconds
        )

    return False


def attribute_key_to_expression_eap_items(attr_key: AttributeKey) -> Expression:
    alias = attr_key.name + "_" + AttributeKey.Type.Name(attr_key.type)
    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise BadSnubaRPCRequestException(
            f"attribute key {attr_key.name} must have a type specified"
        )

    attr_name = ATTRIBUTE_MAPPINGS.get(attr_key.name, attr_key.name)
    if attr_name in NORMALIZED_COLUMNS_EAP_ITEMS:
        if attr_key.type not in NORMALIZED_COLUMNS_EAP_ITEMS[attr_name]:
            formatted_attribute_types = ", ".join(
                map(AttributeKey.Type.Name, NORMALIZED_COLUMNS_EAP_ITEMS[attr_name])
            )
            raise BadSnubaRPCRequestException(
                f"Attribute {attr_key.name} must be one of [{formatted_attribute_types}], got {AttributeKey.Type.Name(attr_key.type)}"
            )

        # To maintain backwards compatibility with the old span_id column, we only need the last 16 characters of the item_id
        if attr_key.name == "sentry.span_id":
            return f.right(column(attr_name[len(COLUMN_PREFIX) :]), 16, alias=alias)

        return f.CAST(
            column(attr_name[len(COLUMN_PREFIX) :]),
            PROTO_TYPE_TO_CLICKHOUSE_TYPE[attr_key.type],
            alias=alias,
        )

    if attr_key.type in PROTO_TYPE_TO_ATTRIBUTE_COLUMN:
        if attr_key.type == AttributeKey.Type.TYPE_BOOLEAN:
            return f.CAST(
                SubscriptableReference(
                    column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attr_key.type]),
                    key=literal(attr_name),
                    alias=None,
                ),
                "Nullable(Boolean)",
                alias=alias,
            )
        elif attr_key.type == AttributeKey.Type.TYPE_INT:
            return f.CAST(
                SubscriptableReference(
                    column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attr_key.type]),
                    key=literal(attr_name),
                    alias=None,
                ),
                "Nullable(Int64)",
                alias=alias,
            )
        return SubscriptableReference(
            column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attr_key.type]),
            key=literal(attr_name),
            alias=alias,
        )

    raise BadSnubaRPCRequestException(
        f"Attribute {attr_key.name} has an unknown type: {AttributeKey.Type.Name(attr_key.type)}"
    )


def apply_virtual_columns_eap_items(
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
            attribute_expression = attribute_key_to_expression_eap_items(
                AttributeKey(
                    name=context.from_column_name,
                    type=NORMALIZED_COLUMNS.get(
                        context.from_column_name, AttributeKey.TYPE_STRING
                    ),
                )
            )
            return f.transform(
                f.CAST(attribute_expression, "String"),
                literals_array(None, [literal(k) for k in context.value_map.keys()]),
                literals_array(None, [literal(v) for v in context.value_map.values()]),
                literal(
                    context.default_value if context.default_value != "" else "unknown"
                ),
                alias=context.to_column_name,
            )

        return expression

    query.transform_expressions(transform_expressions)


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    def _build_label_mapping_key(attr_key: AttributeKey) -> str:
        return attr_key.name + "_" + AttributeKey.Type.Name(attr_key.type)

    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise BadSnubaRPCRequestException(
            f"attribute key {attr_key.name} must have a type specified"
        )
    alias = _build_label_mapping_key(attr_key)

    if attr_key.name == "sentry.trace_id":
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(column("trace_id"), "String", alias=alias)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, got {attr_key.type}"
        )

    if attr_key.name in TIMESTAMP_COLUMNS:
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(
                column(attr_key.name[len("sentry.") :]), "String", alias=alias
            )
        if attr_key.type == AttributeKey.Type.TYPE_INT:
            return f.CAST(column(attr_key.name[len("sentry.") :]), "Int64", alias=alias)
        if (
            attr_key.type == AttributeKey.Type.TYPE_FLOAT
            or attr_key.type == AttributeKey.Type.TYPE_DOUBLE
        ):
            return f.CAST(
                column(attr_key.name[len("sentry.") :]), "Float64", alias=alias
            )
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, float, or integer, got {attr_key.type}"
        )

    if attr_key.name in NORMALIZED_COLUMNS:
        # the second if statement allows Sentry to send TYPE_FLOAT to Snuba when Snuba still has to be backward compatible with TYPE_FLOATS
        if NORMALIZED_COLUMNS[attr_key.name] == attr_key.type or (
            attr_key.type == AttributeKey.Type.TYPE_FLOAT
            and NORMALIZED_COLUMNS[attr_key.name] == AttributeKey.Type.TYPE_DOUBLE
        ):
            return column(attr_key.name[len("sentry.") :], alias=attr_key.name)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as {NORMALIZED_COLUMNS[attr_key.name]}, got {attr_key.type}"
        )

    # End of special handling, just send to the appropriate bucket
    if attr_key.type == AttributeKey.Type.TYPE_STRING:
        return SubscriptableReference(
            alias=alias, column=column("attr_str"), key=literal(attr_key.name)
        )
    if (
        attr_key.type == AttributeKey.Type.TYPE_FLOAT
        or attr_key.type == AttributeKey.Type.TYPE_DOUBLE
    ):
        return SubscriptableReference(
            alias=alias, column=column("attr_num"), key=literal(attr_key.name)
        )
    if attr_key.type == AttributeKey.Type.TYPE_INT:
        return f.CAST(
            SubscriptableReference(
                alias=None, column=column("attr_num"), key=literal(attr_key.name)
            ),
            "Nullable(Int64)",
            alias=alias,
        )
    if attr_key.type == AttributeKey.Type.TYPE_BOOLEAN:
        return f.CAST(
            SubscriptableReference(
                alias=None,
                column=column("attr_num"),
                key=literal(attr_key.name),
            ),
            "Nullable(Boolean)",
            alias=alias,
        )
    raise BadSnubaRPCRequestException(
        f"Attribute {attr_key.name} had an unknown or unset type: {attr_key.type}"
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

        if expression.column.column_name != "attr_str":
            return expression
        context = mapped_column_to_context.get(str(expression.key.value))
        if context:
            attribute_expression = attribute_key_to_expression(
                AttributeKey(
                    name=context.from_column_name,
                    type=NORMALIZED_COLUMNS.get(
                        context.from_column_name, AttributeKey.TYPE_STRING
                    ),
                )
            )
            return f.transform(
                f.CAST(attribute_expression, "String"),
                literals_array(None, [literal(k) for k in context.value_map.keys()]),
                literals_array(None, [literal(v) for v in context.value_map.values()]),
                literal(
                    context.default_value if context.default_value != "" else "unknown"
                ),
                alias=context.to_column_name,
            )

        return expression

    query.transform_expressions(transform_expressions)
