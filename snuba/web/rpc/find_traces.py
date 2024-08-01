import asyncio
from typing import List

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.protobufs.FindTrace_pb2 import FindTraceRequest, FindTraceResponse
from snuba.reader import Reader
from snuba.utils import constants
from snuba.utils.hashes import fnv_1a
from snuba.utils.metrics.timer import Timer


async def _find_traces_matching_filter(
    reader: Reader, filt: FindTraceRequest.Filter
) -> set[str]:
    cond = ""

    if filt.span_with_attr_key_exists.attr_key:
        # TODO this is just a toy example, sql injection etc
        k = filt.span_with_attr_key_exists.attr_key
        bucket_idx = fnv_1a(k.encode("utf-8")) % constants.ATTRIBUTE_BUCKETS
        cond = f"mapContains(attr_str_{bucket_idx}, '{k}') OR mapContains(attr_num_{bucket_idx}, '{k}')"
    elif filt.span_with_attr_key_equals_value.attr_key:
        k = filt.span_with_attr_key_equals_value.attr_key
        v = filt.span_with_attr_key_equals_value.attr_value
        bucket_idx = fnv_1a(k.encode("utf-8")) % constants.ATTRIBUTE_BUCKETS
        # TODO make it work with numbers
        cond = f"attr_str_{bucket_idx}['{k}']='{v}'"

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
