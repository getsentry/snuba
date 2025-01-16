from datetime import datetime, timedelta
from typing import Final, Mapping, Sequence, Set

from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.query import Query
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    column,
    in_cond,
    literal,
    literals_array,
    not_cond,
    or_cond,
)
from snuba.query.expressions import Expression, FunctionCall, SubscriptableReference
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def truncate_request_meta_to_day(meta: RequestMeta) -> None:
    # some tables store timestamp as toStartOfDay(x) in UTC, so if you request 4PM - 8PM on a specific day, nada
    # this changes a request from 4PM - 8PM to a request from midnight today to 8PM tomorrow UTC.
    # it also changes 11PM - 1AM to midnight today to 1AM overmorrow
    start_timestamp = datetime.utcfromtimestamp(meta.start_timestamp.seconds)
    end_timestamp = datetime.utcfromtimestamp(meta.end_timestamp.seconds)
    start_timestamp = start_timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_timestamp = end_timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)

    meta.start_timestamp.seconds = int(start_timestamp.timestamp())
    meta.end_timestamp.seconds = int(end_timestamp.timestamp())


def treeify_or_and_conditions(query: Query) -> None:
    """
    look for expressions like or(a, b, c) and turn them into or(a, or(b, c))
                              and(a, b, c) and turn them into and(a, and(b, c))

    even though clickhouse sql supports arbitrary amount of arguments there are other parts of the
    codebase which assume `or` and `and` have two arguments

    Adding this post-process step is easier than changing the rest of the query pipeline

    Note: does not apply to the conditions of a from_clause subquery (the nested one)
        this is bc transform_expressions is not implemented for composite queries
    """

    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, FunctionCall):
            return exp

        if exp.function_name == "and":
            return combine_and_conditions(exp.parameters)
        elif exp.function_name == "or":
            return combine_or_conditions(exp.parameters)
        else:
            return exp

    query.transform_expressions(transform)


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
        print("attr_keyyy", attr_key.name, attr_key.type)
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
            "Int64",
            alias=alias,
        )
    if attr_key.type == AttributeKey.Type.TYPE_BOOLEAN:
        return f.CAST(
            SubscriptableReference(
                alias=None,
                column=column("attr_num"),
                key=literal(attr_key.name),
            ),
            "Boolean",
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


def trace_item_filters_to_expression(item_filter: TraceItemFilter) -> Expression:
    """
    Trace Item Filters are things like (span.id=12345 AND start_timestamp >= "june 4th, 2024")
    This maps those filters into an expression which can be used in a WHERE clause
    :param item_filter:
    :return:
    """
    if item_filter.HasField("and_filter"):
        filters = item_filter.and_filter.filters
        if len(filters) == 0:
            return literal(True)
        if len(filters) == 1:
            return trace_item_filters_to_expression(filters[0])
        return and_cond(*(trace_item_filters_to_expression(x) for x in filters))

    if item_filter.HasField("or_filter"):
        filters = item_filter.or_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException(
                "Invalid trace item filter, empty 'or' clause"
            )
        if len(filters) == 1:
            return trace_item_filters_to_expression(filters[0])
        return or_cond(*(trace_item_filters_to_expression(x) for x in filters))

    if item_filter.HasField("comparison_filter"):
        k = item_filter.comparison_filter.key
        k_expression = attribute_key_to_expression(k)
        op = item_filter.comparison_filter.op
        v = item_filter.comparison_filter.value

        value_type = v.WhichOneof("value")
        if value_type is None:
            raise BadSnubaRPCRequestException(
                "comparison does not have a right hand side"
            )

        match value_type:
            case "val_bool":
                v_expression: Expression = literal(v.val_bool)
            case "val_str":
                v_expression = literal(v.val_str)
            case "val_float":
                v_expression = literal(v.val_float)
            case "val_double":
                v_expression = literal(v.val_double)
            case "val_int":
                v_expression = literal(v.val_int)
            case "val_null":
                v_expression = literal(None)
            case "val_str_array":
                v_expression = literals_array(
                    None, list(map(lambda x: literal(x), v.val_str_array.values))
                )
            case "val_int_array":
                v_expression = literals_array(
                    None, list(map(lambda x: literal(x), v.val_int_array.values))
                )
            case "val_float_array":
                v_expression = literals_array(
                    None, list(map(lambda x: literal(x), v.val_float_array.values))
                )
            case "val_double_array":
                v_expression = literals_array(
                    None, list(map(lambda x: literal(x), v.val_double_array.values))
                )
            case default:
                raise NotImplementedError(
                    f"translation of AttributeValue type {default} is not implemented"
                )

        if op == ComparisonFilter.OP_EQUALS:
            return f.equals(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_EQUALS:
            return f.notEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_LIKE:
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the LIKE comparison is only supported on string keys"
                )
            return f.like(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_LIKE:
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the NOT LIKE comparison is only supported on string keys"
                )
            return f.notLike(k_expression, v_expression)
        if op == ComparisonFilter.OP_LESS_THAN:
            return f.less(k_expression, v_expression)
        if op == ComparisonFilter.OP_LESS_THAN_OR_EQUALS:
            return f.lessOrEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_GREATER_THAN:
            return f.greater(k_expression, v_expression)
        if op == ComparisonFilter.OP_GREATER_THAN_OR_EQUALS:
            return f.greaterOrEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_IN:
            return in_cond(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_IN:
            return not_cond(in_cond(k_expression, v_expression))

        raise BadSnubaRPCRequestException(
            f"Invalid string comparison, unknown op: {item_filter.comparison_filter}"
        )

    if item_filter.HasField("exists_filter"):
        k = item_filter.exists_filter.key
        if k.name in NORMALIZED_COLUMNS.keys():
            return f.isNotNull(column(k.name))
        if k.type == AttributeKey.Type.TYPE_STRING:
            return f.mapContains(column("attr_str"), literal(k.name))
        else:
            return f.mapContains(column("attr_num"), literal(k.name))

    return literal(True)


def project_id_and_org_conditions(meta: RequestMeta) -> Expression:
    return and_cond(
        in_cond(
            column("project_id"),
            literals_array(
                alias=None,
                literals=[literal(pid) for pid in meta.project_ids],
            ),
        ),
        f.equals(column("organization_id"), meta.organization_id),
    )


def timestamp_in_range_condition(start_ts: int, end_ts: int) -> Expression:
    return and_cond(
        f.less(
            column("timestamp"),
            f.toDateTime(end_ts),
        ),
        f.greaterOrEquals(
            column("timestamp"),
            f.toDateTime(start_ts),
        ),
    )


def base_conditions_and(meta: RequestMeta, *other_exprs: Expression) -> Expression:
    """

    :param meta: The RequestMeta field, common across all RPCs
    :param other_exprs: other expressions to add to the *and* clause
    :return: an expression which looks like (project_id IN (a, b, c) AND organization_id=d AND ...)
    """
    return and_cond(
        project_id_and_org_conditions(meta),
        timestamp_in_range_condition(
            meta.start_timestamp.seconds, meta.end_timestamp.seconds
        ),
        *other_exprs,
    )


def convert_filter_offset(filter_offset: TraceItemFilter) -> Expression:
    if not filter_offset.HasField("comparison_filter"):
        raise TypeError("filter_offset needs to be a comparison filter")
    if filter_offset.comparison_filter.op != ComparisonFilter.OP_GREATER_THAN:
        raise TypeError("filter_offset must use the greater than comparison")

    k_expression = column(filter_offset.comparison_filter.key.name)
    v = filter_offset.comparison_filter.value
    value_type = v.WhichOneof("value")
    if value_type != "val_str":
        raise BadSnubaRPCRequestException("please provide a string for filter offset")

    return f.greater(k_expression, literal(v.val_str))
