from collections import defaultdict
from typing import Any, Callable, Dict, Iterable

from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemColumnValues,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.common.aggregation import ExtrapolationContext


def convert_results(
    request: TraceItemTableRequest, data: Iterable[Dict[str, Any]]
) -> list[TraceItemColumnValues]:
    converters: Dict[str, Callable[[Any], AttributeValue]] = {}

    for column in request.columns:
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
        else:
            raise BadSnubaRPCRequestException(
                "column is not one of: attribute, (conditional) aggregation, or formula"
            )

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

    column_ordering = {column.label: i for i, column in enumerate(request.columns)}

    return list(
        # we return the columns in the order they were requested
        sorted(
            res.values(), key=lambda c: column_ordering.__getitem__(c.attribute_name)
        )
    )
