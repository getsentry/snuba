from typing import List, Optional

from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesRequest,
    TraceItemAttributesResponse,
)
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.common.common import truncate_request_meta_to_day
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


def trace_item_attribute_list_query(
    request: TraceItemAttributesRequest, _timer: Optional[Timer] = None
) -> TraceItemAttributesResponse:
    if request.type == AttributeKey.Type.TYPE_STRING:
        storage = get_storage(StorageKey("spans_str_attrs"))
    elif request.type == AttributeKey.Type.TYPE_FLOAT:
        storage = get_storage(StorageKey("spans_num_attrs"))
    else:
        return TraceItemAttributesResponse(tags=[])

    data_source = storage.get_schema().get_data_source()
    assert isinstance(data_source, TableSource)

    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    truncate_request_meta_to_day(request.meta)

    query = f"""
SELECT DISTINCT attr_key, timestamp
FROM {data_source.get_table_name()}
WHERE organization_id={request.meta.organization_id}
AND project_id IN ({', '.join(str(pid) for pid in request.meta.project_ids)})
AND timestamp BETWEEN fromUnixTimestamp({request.meta.start_timestamp.seconds}) AND fromUnixTimestamp({request.meta.end_timestamp.seconds})
ORDER BY attr_key
LIMIT {request.limit} OFFSET {request.offset}
"""

    cluster = storage.get_cluster()
    reader = cluster.get_reader()
    result = reader.execute(FormattedQuery([StringNode(query)]))

    tags: List[TraceItemAttributesResponse.Tag] = []
    for row in result.get("data", []):
        tags.append(
            TraceItemAttributesResponse.Tag(
                name=row["attr_key"],
                type=request.type,
            )
        )

    return TraceItemAttributesResponse(tags=tags)
