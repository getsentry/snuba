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
from snuba.query.dsl import and_cond, column, in_cond, literal, literals_array
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
    "organization_id": AttributeKey.Type.TYPE_INT,
    "project_id": AttributeKey.Type.TYPE_INT,
    "region": AttributeKey.Type.TYPE_STRING,
    "environment": AttributeKey.Type.TYPE_STRING,
    "uptime_subscription_id": AttributeKey.Type.TYPE_STRING,
    "uptime_check_id": AttributeKey.Type.TYPE_STRING,
    "duration_ms": AttributeKey.Type.TYPE_INT,
    "check_status": AttributeKey.Type.TYPE_STRING,
    "check_status_reason": AttributeKey.Type.TYPE_STRING,
    "http_status_code": AttributeKey.Type.TYPE_INT,
    "trace_id": AttributeKey.Type.TYPE_STRING,
    "retention_days": AttributeKey.Type.TYPE_INT,
}

TIMESTAMP_COLUMNS: Final[Set[str]] = {
    "timestamp",
    "scheduled_check_time",
}


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    def _build_label_mapping_key(attr_key: AttributeKey) -> str:
        return attr_key.name + "_" + AttributeKey.Type.Name(attr_key.type)

    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise BadSnubaRPCRequestException(
            f"attribute key {attr_key.name} must have a type specified"
        )

    alias = _build_label_mapping_key(attr_key)

    if attr_key.name == "trace_id":
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(column("trace_id"), "String", alias=alias)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, got {attr_key.type}"
        )

    if attr_key.name in TIMESTAMP_COLUMNS:
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(column(attr_key.name), "String", alias=alias)
        if attr_key.type == AttributeKey.Type.TYPE_INT:
            return f.CAST(column(attr_key.name), "Int64", alias=alias)
        if (
            attr_key.type == AttributeKey.Type.TYPE_FLOAT
            or attr_key.type == AttributeKey.Type.TYPE_DOUBLE
        ):
            return f.CAST(column(attr_key.name), "Float64", alias=alias)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, float, or integer, got {attr_key.type}"
        )

    if attr_key.name in NORMALIZED_COLUMNS:
        return column(attr_key.name, alias=attr_key.name)

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
