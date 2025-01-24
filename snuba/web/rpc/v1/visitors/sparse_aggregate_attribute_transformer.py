from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ExistsFilter,
    OrFilter,
    TraceItemFilter,
)


class SparseAggregateAttributeTransformer:
    """
    This class is used to transform a TraceItemTableRequest.
    It is meant to make sure that aggregates behave as follows:
    An aggregate like: SELECT animal_type, sum(wing.count) ... GROUP BY animal_type
    should not return
        animal_type   |   sum(wing.count)
        bird          |   64
        chicken       |   12
        dog           |   0
        cat           |   0

    but instead should return
        animal_type   |   sum(wing.count)
        bird          |   64
        chicken       |   12

    why? because the columns for chicken and bird don't have the attribute "wing.count"
    but by default it sets it to 0 when the attribute is not present.

    This class tranforms TraceItemTableRequest adding a filter `hasAttribute("wing.count")`
    for all attributes that are used in aggregate functions. (this ensures the correct behavior as
    described above)

    It does not modify the given request, it returns a new request with the filter added.
    """

    def __init__(self, req: TraceItemTableRequest):
        self.req = req

    def transform(self) -> TraceItemTableRequest:
        # get all the keys that are used in aggregates
        agg_keys = []
        for column in self.req.columns:
            if column.WhichOneof("column") == "aggregation":
                agg_keys.append(column.aggregation.key)

        if len(agg_keys) == 0:
            return self.req
        else:
            # add the exists filters for the agg_keys
            filter_to_add = TraceItemFilter(
                or_filter=OrFilter(
                    filters=[
                        TraceItemFilter(exists_filter=ExistsFilter(key=key))
                        for key in agg_keys
                    ]
                )
            )
            # combine the new filters with the existing one
            if self.req.HasField("filter"):
                new_filter = TraceItemFilter(
                    and_filter=AndFilter(filters=[self.req.filter, filter_to_add])
                )
            else:
                new_filter = filter_to_add

            new_req = TraceItemTableRequest()
            new_req.CopyFrom(self.req)
            new_req.filter.CopyFrom(new_filter)
            return new_req
