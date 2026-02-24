import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TypeVar, cast

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba import settings, state
from snuba.protos.common import MalformedAttributeException
from snuba.protos.common import (
    attribute_key_to_expression as _attribute_key_to_expression,
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


Tin = TypeVar("Tin", bound=ProtobufMessage)
Tout = TypeVar("Tout", bound=ProtobufMessage)

BUCKET_COUNT = 40


def transform_array_value(value: dict[str, str]) -> Any:
    for t, v in value.items():
        if t == "Int":
            return int(v)
        if t == "Double":
            return float(v)
        if t in {"String", "Bool"}:
            return v
    raise BadSnubaRPCRequestException(f"array value type unknown: {type(v)}")


def process_arrays(raw: str) -> dict[str, list[Any]]:
    parsed = json.loads(raw) or {}
    arrays = {}
    for key, values in parsed.items():
        arrays[key] = [transform_array_value(v) for v in values]
    return arrays


def _check_non_string_values_cannot_ignore_case(
    comparison_filter: ComparisonFilter,
) -> None:
    if comparison_filter.ignore_case and (
        comparison_filter.value.WhichOneof("value") != "val_str"
        and comparison_filter.value.WhichOneof("value") != "val_str_array"
    ):
        raise BadSnubaRPCRequestException("Cannot ignore case on non-string values")


def next_monday(dt: datetime) -> datetime:
    return dt + timedelta(days=(7 - dt.weekday()) or 7)


def prev_monday(dt: datetime) -> datetime:
    return dt - timedelta(days=(dt.weekday() % 7))


def truncate_request_meta_to_day(meta: RequestMeta) -> None:
    # some tables store timestamp as toStartOfDay(x) in UTC, so if you request 4PM - 8PM on a specific day, nada
    # this changes a request from 4PM - 8PM to a request from midnight today to 8PM tomorrow UTC.
    # it also changes 11PM - 1AM to midnight today to 1AM overmorrow
    start_timestamp = datetime.utcfromtimestamp(meta.start_timestamp.seconds)
    end_timestamp = datetime.utcfromtimestamp(meta.end_timestamp.seconds)
    start_timestamp = start_timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_timestamp = end_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        days=1
    )

    meta.start_timestamp.seconds = int(start_timestamp.timestamp())
    meta.end_timestamp.seconds = int(end_timestamp.timestamp())


def use_sampling_factor(meta: RequestMeta) -> bool:
    """
    Since we started writing the sampling factor on a specific date, we should only use it on queries that start after that date.
    """
    use_sampling_factor_timestamp_seconds = cast(
        int,
        state.get_int_config(
            "use_sampling_factor_timestamp_seconds",
            settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS,
        ),
    )
    if use_sampling_factor_timestamp_seconds == 0:
        return False

    return meta.start_timestamp.seconds >= use_sampling_factor_timestamp_seconds


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


def add_existence_check_to_subscriptable_references(query: Query) -> None:
    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, SubscriptableReference):
            return exp

        return FunctionCall(
            alias=exp.alias,
            function_name="if",
            parameters=(
                f.mapContains(exp.column, exp.key),
                SubscriptableReference(None, exp.column, exp.key),
                literal(None),
            ),
        )

    query.transform_expressions(transform)


def trace_item_filters_to_expression(
    item_filter: TraceItemFilter,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression:
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
        elif len(filters) == 1:
            return trace_item_filters_to_expression(filters[0], attribute_key_to_expression)
        return and_cond(
            *(trace_item_filters_to_expression(x, attribute_key_to_expression) for x in filters)
        )

    if item_filter.HasField("or_filter"):
        filters = item_filter.or_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'or' clause")
        elif len(filters) == 1:
            return trace_item_filters_to_expression(filters[0], attribute_key_to_expression)
        return or_cond(
            *(trace_item_filters_to_expression(x, attribute_key_to_expression) for x in filters)
        )

    if item_filter.HasField("not_filter"):
        filters = item_filter.not_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'not' clause")
        elif len(filters) == 1:
            return not_cond(
                trace_item_filters_to_expression(filters[0], attribute_key_to_expression)
            )
        return not_cond(
            and_cond(
                *(trace_item_filters_to_expression(x, attribute_key_to_expression) for x in filters)
            )
        )

    if item_filter.HasField("comparison_filter"):
        k = item_filter.comparison_filter.key
        k_expression = attribute_key_to_expression(k)
        op = item_filter.comparison_filter.op
        v = item_filter.comparison_filter.value

        value_type = v.WhichOneof("value")
        if value_type is None:
            raise BadSnubaRPCRequestException("comparison does not have a right hand side")

        if v.is_null:
            v_expression: Expression = literal(None)
        else:
            match value_type:
                case "val_bool":
                    v_expression = literal(v.val_bool)
                case "val_str":
                    v_expression = literal(v.val_str)
                case "val_float":
                    v_expression = literal(v.val_float)
                case "val_double":
                    v_expression = literal(v.val_double)
                case "val_int":
                    v_expression = literal(v.val_int)
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
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            expr = (
                f.equals(f.lower(k_expression), f.lower(v_expression))
                if item_filter.comparison_filter.ignore_case
                else f.equals(k_expression, v_expression)
            )
            # we redefine the way equals works for nulls
            # now null=null is true
            expr_with_null = or_cond(expr, and_cond(f.isNull(k_expression), f.isNull(v_expression)))
            return expr_with_null
        if op == ComparisonFilter.OP_NOT_EQUALS:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            expr = (
                f.notEquals(f.lower(k_expression), f.lower(v_expression))
                if item_filter.comparison_filter.ignore_case
                else f.notEquals(k_expression, v_expression)
            )
            # we redefine the way not equals works for nulls
            # now null!=null is true
            expr_with_null = or_cond(expr, f.xor(f.isNull(k_expression), f.isNull(v_expression)))
            return expr_with_null
        if op == ComparisonFilter.OP_LIKE:
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the LIKE comparison is only supported on string keys"
                )
            comparison_function = f.ilike if item_filter.comparison_filter.ignore_case else f.like
            return comparison_function(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_LIKE:
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the NOT LIKE comparison is only supported on string keys"
                )
            comparison_function = (
                f.notILike if item_filter.comparison_filter.ignore_case else f.notLike
            )
            expr = comparison_function(k_expression, v_expression)
            # we redefine the way not like works for nulls
            # now null not like "%anything%" is true
            expr_with_null = or_cond(expr, f.isNull(k_expression))
            return expr_with_null
        if op == ComparisonFilter.OP_LESS_THAN:
            return f.less(k_expression, v_expression)
        if op == ComparisonFilter.OP_LESS_THAN_OR_EQUALS:
            return f.lessOrEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_GREATER_THAN:
            return f.greater(k_expression, v_expression)
        if op == ComparisonFilter.OP_GREATER_THAN_OR_EQUALS:
            return f.greaterOrEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_IN:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            if item_filter.comparison_filter.ignore_case:
                k_expression = f.lower(k_expression)
                v_expression = literals_array(
                    None,
                    list(map(lambda x: literal(x.lower()), v.val_str_array.values)),
                )
            expr = in_cond(k_expression, v_expression)
            # note: v_expression must be an array
            # we redefine the way in works for nulls
            # now null in ['hi', null] is true
            expr_with_null = or_cond(
                expr,
                and_cond(f.isNull(k_expression), f.has(v_expression, literal(None))),
            )
            return expr_with_null
        if op == ComparisonFilter.OP_NOT_IN:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            if item_filter.comparison_filter.ignore_case:
                k_expression = f.lower(k_expression)
                v_expression = literals_array(
                    None,
                    list(map(lambda x: literal(x.lower()), v.val_str_array.values)),
                )
            expr = not_cond(in_cond(k_expression, v_expression))
            # note: v_expression must be an array
            # we redefine the way not in works for nulls
            # now null not in ['hi'] is true
            expr_with_null = or_cond(
                expr,
                and_cond(
                    f.isNull(k_expression),
                    not_cond(f.has(v_expression, literal(None))),
                ),
            )
            return expr_with_null

        raise BadSnubaRPCRequestException(
            f"Invalid string comparison, unknown op: {item_filter.comparison_filter}"
        )

    if item_filter.HasField("exists_filter"):
        return get_field_existence_expression(
            attribute_key_to_expression(item_filter.exists_filter.key)
        )

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
            f.toDateTime(
                datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            ),
        ),
        f.greaterOrEquals(
            column("timestamp"),
            f.toDateTime(
                datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            ),
        ),
    )


def valid_sampling_factor_conditions() -> Expression:
    return and_cond(
        f.lessOrEquals(column("sampling_factor"), 1), f.greater(column("sampling_factor"), 0)
    )


def base_conditions_and(meta: RequestMeta, *other_exprs: Expression) -> Expression:
    """

    :param meta: The RequestMeta field, common across all RPCs
    :param other_exprs: other expressions to add to the *and* clause
    :return: an expression which looks like (project_id IN (a, b, c) AND organization_id=d AND ...)
    """
    return and_cond(
        project_id_and_org_conditions(meta),
        timestamp_in_range_condition(meta.start_timestamp.seconds, meta.end_timestamp.seconds),
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


def get_field_existence_expression(field: Expression) -> Expression:
    def get_subscriptable_field(field: Expression) -> SubscriptableReference | None:
        """
        Check if the field is a subscriptable reference or a function call with a subscriptable reference as the first parameter to handle the case
        where the field is casting a subscriptable reference (e.g. for integers). If so, return the subscriptable reference.
        """
        if isinstance(field, SubscriptableReference):
            return field
        elif isinstance(field, FunctionCall) and len(field.parameters) > 0:
            if len(field.parameters) > 0 and isinstance(
                field.parameters[0], SubscriptableReference
            ):
                return field.parameters[0]

        return None

    subscriptable_field = get_subscriptable_field(field)
    if subscriptable_field is not None:
        return f.mapContains(subscriptable_field.column, subscriptable_field.key)

    if isinstance(field, FunctionCall) and field.function_name == "arrayElement":
        return f.mapContains(field.parameters[0], field.parameters[1])

    if isinstance(field, FunctionCall) and field.function_name == "arrayMap":
        # Array attributes in the JSON column return empty arrays (not NULL)
        # for missing keys, so notEmpty is the correct existence check.
        return f.notEmpty(field)

    return f.isNotNull(field)
