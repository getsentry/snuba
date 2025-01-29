from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

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
from snuba.query.expressions import Expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.attribute_key_to_expression import (
    attribute_key_to_expression,
)


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
            case default:
                raise NotImplementedError(
                    f"translation of AttributeValue type {default} is not implemented"
                )

        if op == ComparisonFilter.OP_EQUALS:
            return (
                f.equals(f.lower(k_expression), f.lower(v_expression))
                if item_filter.comparison_filter.ignore_case
                else f.equals(k_expression, v_expression)
            )
        if op == ComparisonFilter.OP_NOT_EQUALS:
            return (
                f.notEquals(f.lower(k_expression), f.lower(v_expression))
                if item_filter.comparison_filter.ignore_case
                else f.notEquals(k_expression, v_expression)
            )
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
            if item_filter.comparison_filter.ignore_case:
                k_expression = f.lower(k_expression)
                v_expression = literals_array(
                    None,
                    list(map(lambda x: literal(x.lower()), v.val_str_array.values)),
                )
            return in_cond(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_IN:
            if item_filter.comparison_filter.ignore_case:
                k_expression = f.lower(k_expression)
                v_expression = literals_array(
                    None,
                    list(map(lambda x: literal(x.lower()), v.val_str_array.values)),
                )
            return not_cond(in_cond(k_expression, v_expression))

        raise BadSnubaRPCRequestException(
            f"Invalid string comparison, unknown op: {item_filter.comparison_filter}"
        )

    if item_filter.HasField("exists_filter"):
        k = item_filter.exists_filter.key
        if k.type == AttributeKey.Type.TYPE_STRING:
            return f.mapContains(column("attr_string"), literal(k.name))
        elif k.type == AttributeKey.Type.TYPE_INT:
            return f.mapContains(column("attr_int"), literal(k.name))
        elif k.type == AttributeKey.Type.TYPE_BOOLEAN:
            return f.mapContains(column("attr_bool"), literal(k.name))
        elif k.type == AttributeKey.Type.TYPE_FLOAT:
            return f.mapContains(column("attr_double"), literal(k.name))

    return literal(True)
