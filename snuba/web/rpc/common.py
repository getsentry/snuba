from typing import Final, Mapping, Set

from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1alpha.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.query import Query
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, literal, literals_array, or_cond
from snuba.query.expressions import Expression, FunctionCall, SubscriptableReference
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


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
    "service": AttributeKey.Type.TYPE_STRING,
    "span_id": AttributeKey.Type.TYPE_INT,
    "parent_span_id": AttributeKey.Type.TYPE_INT,
    "segment_id": AttributeKey.Type.TYPE_INT,
    "segment_name": AttributeKey.Type.TYPE_STRING,
    "is_segment": AttributeKey.Type.TYPE_BOOLEAN,
    "duration_ms": AttributeKey.Type.TYPE_INT,
    "exclusive_time_ms": AttributeKey.Type.TYPE_INT,
    "retention_days": AttributeKey.Type.TYPE_INT,
    "name": AttributeKey.Type.TYPE_STRING,
    "sample_weight": AttributeKey.Type.TYPE_FLOAT,
    "timestamp": AttributeKey.Type.TYPE_UNSPECIFIED,
    "start_timestamp": AttributeKey.Type.TYPE_UNSPECIFIED,
    "end_timestamp": AttributeKey.Type.TYPE_UNSPECIFIED,
}

# Columns stored as integers that are usually presented to users as hex strings
HEX_ID_COLUMNS: Final[Set[str]] = {"span_id", "parent_span_id", "segment_id"}

TIMESTAMP_COLUMNS: Final[Set[str]] = {"timestamp", "start_timestamp", "end_timestamp"}


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise BadSnubaRPCRequestException(
            f"attribute key {attr_key.name} must have a type specified"
        )

    if attr_key.name == "trace_id":
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(column("trace_id"), "String", alias="trace_id")
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, got {attr_key.type}"
        )

    if attr_key.name in HEX_ID_COLUMNS:
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.hex(column(attr_key.name), alias=attr_key.name)
        if attr_key.type == AttributeKey.Type.TYPE_INT:
            return f.CAST(column(attr_key.name), "UInt64", alias=attr_key.name)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, got {attr_key.type}"
        )

    if attr_key.name in TIMESTAMP_COLUMNS:
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(column(attr_key.name), "String", alias=attr_key.name)
        if attr_key.type == AttributeKey.Type.TYPE_INT:
            return f.CAST(column(attr_key.name), "Int64", alias=attr_key.name)
        if attr_key.type == AttributeKey.Type.TYPE_FLOAT:
            return f.CAST(column(attr_key.name), "Float64", alias=attr_key.name)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, float, or integer, got {attr_key.type}"
        )

    if attr_key.name in NORMALIZED_COLUMNS:
        if NORMALIZED_COLUMNS[attr_key.name] == attr_key.type:
            return column(attr_key.name)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as {NORMALIZED_COLUMNS[attr_key.name]}, got {attr_key.type}"
        )

    # End of special handling, just send to the appropriate bucket
    if attr_key.type == AttributeKey.Type.TYPE_STRING:
        return SubscriptableReference(
            alias=attr_key.name, column=column("attr_str"), key=literal(attr_key.name)
        )
    if attr_key.type == AttributeKey.Type.TYPE_FLOAT:
        return SubscriptableReference(
            alias=attr_key.name, column=column("attr_num"), key=literal(attr_key.name)
        )
    if attr_key.type == AttributeKey.Type.TYPE_INT:
        return f.CAST(
            SubscriptableReference(
                alias=attr_key.name,
                column=column("attr_num"),
                key=literal(attr_key.name),
            ),
            "Int64",
        )
    if attr_key.type == AttributeKey.Type.TYPE_BOOLEAN:
        return f.CAST(
            SubscriptableReference(
                alias=attr_key.name,
                column=column("attr_num"),
                key=literal(attr_key.name),
            ),
            "Boolean",
        )
    raise BadSnubaRPCRequestException(
        f"Attribute {attr_key.name} had an unknown or unset type: {attr_key.type}"
    )


def trace_item_filters_to_expression(item_filter: TraceItemFilter) -> Expression:
    """
    Trace Item Filters are things like (span.id=12345 AND start_timestamp >= "june 4th, 2024")
    This maps those filters into an expression which can be used in a WHERE clause
    :param item_filter:
    :return:
    """
    if item_filter.and_filter:
        filters = item_filter.and_filter.filters
        if len(filters) == 0:
            return literal(True)
        if len(filters) == 1:
            return trace_item_filters_to_expression(filters[0])
        return and_cond(*(trace_item_filters_to_expression(x) for x in filters))

    if item_filter.or_filter:
        filters = item_filter.or_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException(
                "Invalid trace item filter, empty 'or' clause"
            )
        if len(filters) == 1:
            return trace_item_filters_to_expression(filters[0])
        return or_cond(*(trace_item_filters_to_expression(x) for x in filters))

    if item_filter.comparison_filter:
        k = item_filter.comparison_filter.key
        k_expression = attribute_key_to_expression(k)
        op = item_filter.comparison_filter.op
        v = item_filter.comparison_filter.value

        value_type = v.WhichOneof("value")
        if value_type is None:
            raise BadSnubaRPCRequestException(
                "comparison does not have a right hand side"
            )

        v_expression = {
            "val_bool": literal(v.val_bool),
            "val_str": literal(v.val_str),
            "val_float": literal(v.val_float),
            "val_int": literal(v.val_int),
        }[value_type]

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

        raise BadSnubaRPCRequestException(
            "Invalid string comparison, unknown op: ", item_filter.comparison_filter
        )

    if item_filter.exists_filter:
        k = item_filter.exists_filter.key
        if k.name in NORMALIZED_COLUMNS.keys():
            return f.isNotNull(column(k.name))
        if k.type == AttributeKey.Type.TYPE_STRING:
            # TODO: this doesn't actually work yet, need to make mapContains work with hash mapper too
            return f.mapContains(column("attr_str"), literal(k.name))
        else:
            return f.mapContains(column("attr_num"), literal(k.name))

    raise Exception("Unknown filter: ", item_filter)


def base_conditions_and(meta: RequestMeta, *other_exprs: Expression) -> Expression:
    """

    :param meta: The RequestMeta field, common across all RPCs
    :param other_exprs: other expressions to add to the *and* clause
    :return: an expression which looks like (project_id IN (a, b, c) AND organization_id=d AND ...)
    """
    return and_cond(
        in_cond(
            column("project_id"),
            literals_array(
                alias=None,
                literals=[literal(pid) for pid in meta.project_ids],
            ),
        ),
        f.equals(column("organization_id"), meta.organization_id),
        f.less(
            column("timestamp"),
            f.toDateTime(meta.end_timestamp.seconds),
        ),
        f.greaterOrEquals(
            column("timestamp"),
            f.toDateTime(meta.start_timestamp.seconds),
        ),
        *other_exprs,
    )