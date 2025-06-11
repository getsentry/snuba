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
    Reliability,
)

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.common.aggregation import ExtrapolationContext


def add_converter(
    column: Column, converters: Dict[str, Callable[[Any], AttributeValue]]
) -> None:
    if column.HasField("key"):
        if column.key.type == AttributeKey.TYPE_BOOLEAN:
            converters[column.label] = lambda x: AttributeValue(val_bool=bool(x))
        elif column.key.type == AttributeKey.TYPE_STRING:
            converters[column.label] = lambda x: AttributeValue(val_str=str(x))
        elif column.key.type == AttributeKey.TYPE_INT:
            converters[column.label] = lambda x: AttributeValue(val_int=int(x))
        elif column.key.type == AttributeKey.TYPE_FLOAT:
            converters[column.label] = lambda x: AttributeValue(val_float=float(x))
        elif column.key.type == AttributeKey.TYPE_DOUBLE:
            converters[column.label] = lambda x: AttributeValue(val_double=float(x))
    elif column.HasField("conditional_aggregation"):
        converters[column.label] = lambda x: AttributeValue(val_double=float(x))
    elif column.HasField("formula"):
        converters[column.label] = lambda x: AttributeValue(val_double=float(x))
        add_converter(column.formula.left, converters)
        add_converter(column.formula.right, converters)
    else:
        raise BadSnubaRPCRequestException(
            "column is not one of: attribute, (conditional) aggregation, or formula"
        )


def convert_results(
    request: TraceItemTableRequest, data: Iterable[Dict[str, Any]]
) -> list[TraceItemColumnValues]:
    converters: Dict[str, Callable[[Any], AttributeValue]] = {}
    for column in request.columns:
        add_converter(column, converters)

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
                    res[column_name].reliabilities.append(
                        extrapolation_context.reliability
                    )

    # add formula reliabilities, remove the left and right parts
    for column in request.columns:
        if column.HasField("formula"):
            # compute its reliability based on the reliabilities of the left and right parts
            # (already computed in res variable)
            # a formula is reliable iff all of its parts are reliable (.left and .right)
            # ex: (agg1 + agg2) / agg3 * agg4 is reliable iff agg1, agg2, agg3, agg4 are reliable
            reliable_so_far: list[Reliability.ValueType] = []
            for resname, resvalue in res.items():
                if re.fullmatch(
                    rf"{re.escape(column.label)}(\.left|\.right)+", resname
                ):
                    for i, reliability in enumerate(resvalue.reliabilities):
                        if len(reliable_so_far) <= i:
                            # bc we are extending as we go, it should only ever be 1 behind
                            assert i == len(reliable_so_far)
                            reliable_so_far.append(reliability)
                        else:
                            if reliable_so_far[i] in [
                                Reliability.RELIABILITY_UNSPECIFIED,
                            ]:
                                # these wont change so we skip (if any part of the formula has
                                # a unspecified reliability, the formula will have a unspecified reliability
                                continue

                            if reliability not in [
                                Reliability.RELIABILITY_UNSPECIFIED,
                                Reliability.RELIABILITY_LOW,
                                Reliability.RELIABILITY_HIGH,
                            ]:
                                raise ValueError(f"Invalid reliability: {reliability}")

                            if reliability == Reliability.RELIABILITY_LOW:
                                reliable_so_far[i] = Reliability.RELIABILITY_LOW
                            elif (
                                reliability == Reliability.RELIABILITY_HIGH
                                and reliable_so_far[i] != Reliability.RELIABILITY_LOW
                            ):
                                reliable_so_far[i] = Reliability.RELIABILITY_HIGH
                            elif reliability == Reliability.RELIABILITY_UNSPECIFIED:
                                reliable_so_far[i] = Reliability.RELIABILITY_UNSPECIFIED
            # set reliability of the formula to be the newly calculated ones
            while len(res[column.label].reliabilities) > 0:
                res[column.label].reliabilities.pop()
            for e in reliable_so_far:
                assert e is not None
                res[column.label].reliabilities.append(e)

    # we want to remove any columns that were not requested by the user.
    # we have added extra columns such as ones to compute formula reliabilities
    # that we dont want to return to the user
    requested_column_labels = set(e.label for e in request.columns)
    to_delete = list(filter(lambda k: k not in requested_column_labels, res.keys()))
    for name in to_delete:
        del res[name]

    column_ordering = {column.label: i for i, column in enumerate(request.columns)}

    return list(
        # we return the columns in the order they were requested
        sorted(
            res.values(), key=lambda c: column_ordering.__getitem__(c.attribute_name)
        )
    )
