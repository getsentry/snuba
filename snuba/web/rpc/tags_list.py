from typing import List, Optional

from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TagsListRequest,
    TagsListResponse,
)

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.datasets.plans.storage_processing import get_query_data_source
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


def tags_list_query(
    request: TagsListRequest, _timer: Optional[Timer] = None
) -> TagsListResponse:
    str_storage = get_storage(StorageKey("spans_str_attrs"))
    num_storage = get_storage(StorageKey("spans_num_attrs"))

    str_from = get_query_data_source(
        str_storage.get_schema().get_data_source(),
        allocation_policies=str_storage.get_allocation_policies(),
        final=False,
        sampling_rate=None,
        storage_key=str_storage.get_storage_key(),
    )

    num_from = get_query_data_source(
        num_storage.get_schema().get_data_source(),
        allocation_policies=num_storage.get_allocation_policies(),
        final=False,
        sampling_rate=None,
        storage_key=num_storage.get_storage_key(),
    )

    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    query = f"""
SELECT * FROM (
    SELECT DISTINCT attr_key, 'str' as type
    FROM {str_from.table_name}
    UNION ALL
    SELECT DISTINCT attr_key, 'num' as type
    FROM {num_from.table_name}
) LIMIT {request.limit} OFFSET {request.offset}
SETTINGS max_bytes_to_read=1000000000
"""

    cluster = str_storage.get_cluster()
    reader = cluster.get_reader()
    result = reader.execute(FormattedQuery([StringNode(query)]))

    tags: List[TagsListResponse.Tag] = []
    for row in result.get("data", []):
        tags.append(
            TagsListResponse.Tag(
                name=row["attr_key"],
                type={
                    "str": TagsListResponse.TYPE_STRING,
                    "num": TagsListResponse.TYPE_NUMBER,
                }[row["type"]],
            )
        )

    return TagsListResponse(tags=tags)
