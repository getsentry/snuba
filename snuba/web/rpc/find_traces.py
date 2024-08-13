import asyncio
from typing import List

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.protobufs.Filters_pb2 import NumericalFilter, StringFilter, TraceItemFilter
from snuba.protobufs.FindTrace_pb2 import FindTraceRequest, FindTraceResponse
from snuba.reader import Reader
from snuba.utils import constants
from snuba.utils.hashes import fnv_1a
from snuba.utils.metrics.timer import Timer


async def _find_traces_matching_filter(
    reader: Reader, filt: TraceItemFilter
) -> set[str]:
    cond = ""

    # TODO this is just a toy example, sql injection etc
    if filt.exists:
        k = filt.span_filter.exists.tag_name
        bucket_idx = fnv_1a(k.encode("utf-8")) % constants.ATTRIBUTE_BUCKETS
        cond = f"mapContains(attr_str_{bucket_idx}, '{k}') OR mapContains(attr_num_{bucket_idx}, '{k}')"
    elif filt.string_comparison:
        k = filt.span_filter.string_comparison.tag_name
        op = filt.span_filter.string_comparison.op
        v = filt.span_filter.string_comparison.value
        bucket_idx = fnv_1a(k.encode("utf-8")) % constants.ATTRIBUTE_BUCKETS

        op_map = {
            StringFilter.EQUALS: "=",
            StringFilter.NOT_EQUALS: "<>",
            StringFilter.LIKE: " LIKE ",
            StringFilter.NOT_LIKE: " NOT LIKE ",
        }

        cond = f"attr_str_{bucket_idx}['{k}']{op_map[op]}'{v}'"
    elif filt.number_comparison:
        k = filt.span_filter.number_comparison.tag_name
        op = filt.span_filter.number_comparison.op
        v = filt.span_filter.number_comparison.value
        bucket_idx = fnv_1a(k.encode("utf-8")) % constants.ATTRIBUTE_BUCKETS

        op_map = {
            NumericalFilter.EQUALS: "=",  # TODO: float equality is finnicky, we might want to do |a-b|<epsilon
            NumericalFilter.NOT_EQUALS: "<>",
            NumericalFilter.LESS_THAN: "<",
            NumericalFilter.LESS_THAN_OR_EQUALS: "<=",
            NumericalFilter.GREATER_THAN: ">",
            NumericalFilter.GREATER_THAN_OR_EQUALS: ">=",
        }
        cond = f"attr_num_{bucket_idx}['{k}']{op_map[op]}{v}"

    query = f"""
SELECT trace_id
FROM eap_spans_local
WHERE ({cond})
        """
    res = reader.execute(
        FormattedQuery([StringNode(query)]),
        robust=True,
    )

    return set(x["trace_id"] for x in res["data"])


async def find_traces(req: FindTraceRequest, timer: Timer) -> FindTraceResponse:
    return FindTraceResponse()
    if len(req.filters) == 0:
        return FindTraceResponse()

    if len(req.filters) > 5:
        return FindTraceResponse()  # todo: better error handling

    storage = get_storage(StorageKey("eap_spans"))
    reader = storage.get_cluster().get_reader()

    results: List[set[str]] = await asyncio.gather(
        *(_find_traces_matching_filter(reader, filt) for filt in req.filters)
    )

    uuids = set.intersection(*results)

    return FindTraceResponse(trace_uuids=uuids)
