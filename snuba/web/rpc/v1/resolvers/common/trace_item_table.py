import re
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable

from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    Function,
    Reliability,
    StrArray,
)

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.common.aggregation import ExtrapolationContext


def _get_converter_for_type(
    key_type: "AttributeKey.Type.ValueType",
) -> Callable[[Any], AttributeValue]:
    """Returns a converter function for the given attribute type."""
    if key_type == AttributeKey.TYPE_BOOLEAN:
        return lambda x: AttributeValue(val_bool=bool(x))
    elif key_type == AttributeKey.TYPE_STRING:
        return lambda x: AttributeValue(val_str=str(x))
    elif key_type == AttributeKey.TYPE_INT:
        return lambda x: AttributeValue(val_int=int(x))
    elif key_type == AttributeKey.TYPE_FLOAT:
        return lambda x: AttributeValue(val_float=float(x))
    elif key_type == AttributeKey.TYPE_DOUBLE:
        return lambda x: AttributeValue(val_double=float(x))
    elif key_type == AttributeKey.TYPE_ARRAY:
        return lambda x: AttributeValue(
            val_str_array=StrArray(values=[str(v) for v in x] if x else [])
        )
    else:
        raise BadSnubaRPCRequestException(
            f"unknown attribute type: {AttributeKey.Type.Name(key_type)}"
        )


def _get_double_converter() -> Callable[[Any], AttributeValue]:
    """Returns a converter that converts to double (used for most aggregations)."""
    return lambda x: AttributeValue(val_double=float(x))


def _add_converter(column: Column, converters: Dict[str, Callable[[Any], AttributeValue]]) -> None:
    if column.HasField("key"):
        converters[column.label] = _get_converter_for_type(column.key.type)
    elif column.HasField("aggregation"):
        # For FUNCTION_ANY, the result type matches the key type since it returns actual values
        if column.aggregation.aggregate == Function.FUNCTION_ANY:
            converters[column.label] = _get_converter_for_type(column.aggregation.key.type)
        else:
            # Other aggregation functions return numeric values
            converters[column.label] = _get_double_converter()
    elif column.HasField("conditional_aggregation"):
        # For FUNCTION_ANY, the result type matches the key type since it returns actual values
        # Note: AggregationToConditionalAggregationVisitor converts aggregation -> conditional_aggregation
        if column.conditional_aggregation.aggregate == Function.FUNCTION_ANY:
            converters[column.label] = _get_converter_for_type(
                column.conditional_aggregation.key.type
            )
        else:
            # Other aggregation functions return numeric values
            converters[column.label] = _get_double_converter()
    elif column.HasField("formula"):
        converters[column.label] = _get_double_converter()
        _add_converter(column.formula.left, converters)
        _add_converter(column.formula.right, converters)
    elif column.HasField("literal"):
        converters[column.label] = _get_double_converter()
    else:
        raise BadSnubaRPCRequestException(
            "column is not one of: attribute, (conditional) aggregation, or formula"
        )


def get_converters_for_columns(
    columns: Iterable[Column],
) -> Dict[str, Callable[[Any], AttributeValue]]:
    """
    Returns a dictionary of column labels to their corresponding converters.
    Converters are functions that convert a value returned by a clickhouse query to an AttributeValue.
    """
    converters: Dict[str, Callable[[Any], AttributeValue]] = {}
    for column in columns:
        _add_converter(column, converters)
    return converters


def _is_sub_column(result_column_name: str, column: Column) -> bool:
    """
    returns true if result_column_name is a sub column of column. false otherwise.
    """
    # this logic could theoretically cause issue if the user passes in such a column label to a non-subcolumn.
    # for now, we assume that the user will not do this.
    return bool(re.fullmatch(rf"{re.escape(column.label)}(\.left|\.right)+", result_column_name))


def _get_reliabilities_for_formula(
    column: Column, res: Dict[str, TraceItemColumnValues]
) -> list[Reliability.ValueType]:
    """
    Compute and return the reliabilities for the given formula column,
    based on the reliabilities of the left and right parts.

    Ex:
    When users send a request with a formula such as sum(B)/min(B)
    we also separately query for sum(B), min(B) separately (earlier in the codebase).
    Thus, we already have the reliabilities for sum(B), min(B) in res labels as .left and .right.
    We use them in this function to compute the reliability of the formula, based on the following:
    a formula is reliable iff all of its parts are reliable (.left and .right)
    ex: (agg1 + agg2) / agg3 * agg4 is reliable iff agg1, agg2, agg3, agg4 are reliable.

    Select A, sum(B)/min(B) AS agg GROUP BY A
    +----+--------------+----------+--------------+
    | A  |     agg      | agg.left |  agg.right   |
    +----+--------------+----------+--------------+
    | A1 | reliable     | reliable | reliable     |
    | A2 | not reliable | reliable | not reliable |
    | A3 | reliable     | reliable | reliable     |
    +----+--------------+----------+--------------+
    you can see that each column has a reliability for each group by. and the reliabilities of agg is determined
    based on the reliabilities of agg.left and agg.right. In this case the function would return
    [reliable, not reliable, reliable]
    """

    reliable_so_far: list[Reliability.ValueType] = []
    for resname, resvalue in res.items():
        if _is_sub_column(resname, column):
            for i, reliability in enumerate(resvalue.reliabilities):
                if len(reliable_so_far) <= i:
                    # bc we are extending as we go, it should only ever be 1 behind
                    assert i == len(reliable_so_far)
                    reliable_so_far.append(reliability)
                else:
                    if reliability not in [
                        Reliability.RELIABILITY_UNSPECIFIED,
                        Reliability.RELIABILITY_LOW,
                        Reliability.RELIABILITY_HIGH,
                    ]:
                        raise ValueError(f"Invalid reliability: {reliability}")
                    reliable_so_far[i] = min(reliable_so_far[i], reliability)
    return reliable_so_far


def convert_results(
    request: TraceItemTableRequest, data: Iterable[Dict[str, Any]]
) -> list[TraceItemColumnValues]:
    converters = get_converters_for_columns(request.columns)

    res: defaultdict[str, TraceItemColumnValues] = defaultdict(TraceItemColumnValues)
    for row in data:
        for column_name, value in row.items():
            if column_name in converters.keys():
                extrapolation_context = ExtrapolationContext.from_row(column_name, row)
                res[column_name].attribute_name = column_name
                if value is None:
                    res[column_name].results.append(AttributeValue(is_null=True))
                else:
                    res[column_name].results.append(converters[column_name](value))

                if extrapolation_context.is_extrapolated:
                    res[column_name].reliabilities.append(extrapolation_context.reliability)

    # add formula reliabilities, remove the left and right parts
    for column in request.columns:
        if column.HasField("formula") and column.label in res:
            # compute the reliabilities for the formula
            reliabilities = _get_reliabilities_for_formula(column, res)
            # set the reliabilities of the formula to be the ones we calculated
            while len(res[column.label].reliabilities) > 0:
                res[column.label].reliabilities.pop()
            for e in reliabilities:
                assert e is not None
                res[column.label].reliabilities.append(e)

    # remove any columns that were not explicitly requested by the user in the request
    requested_column_labels = set(e.label for e in request.columns)
    to_delete = list(filter(lambda k: k not in requested_column_labels, res.keys()))
    for name in to_delete:
        del res[name]

    column_ordering = {column.label: i for i, column in enumerate(request.columns)}

    return list(
        # we return the columns in the order they were requested
        sorted(res.values(), key=lambda c: column_ordering.__getitem__(c.attribute_name))
    )
