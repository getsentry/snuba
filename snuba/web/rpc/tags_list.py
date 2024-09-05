from datetime import datetime
from typing import List, Optional

from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TagsListRequest,
    TagsListResponse,
)

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


def tags_list_query(
    request: TagsListRequest, _timer: Optional[Timer] = None
) -> TagsListResponse:
    str_storage = get_storage(StorageKey("spans_str_attrs"))
    num_storage = get_storage(StorageKey("spans_num_attrs"))

    str_data_source = str_storage.get_schema().get_data_source()
    assert isinstance(str_data_source, TableSource)
    num_data_source = num_storage.get_schema().get_data_source()
    assert isinstance(num_data_source, TableSource)

    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    start_timestamp = datetime.utcfromtimestamp(request.meta.start_timestamp.seconds)
    if start_timestamp.day >= datetime.now().day and start_timestamp.hour != 0:
        raise BadSnubaRPCRequestException(
            "Tags' timestamps are stored per-day, you probably want to set start_timestamp to UTC 00:00 today or a time yesterday."
        )

    query = f"""
SELECT * FROM (
    SELECT DISTINCT attr_key, 'str' as type, timestamp
    FROM {str_data_source.get_table_name()}
    WHERE organization_id={request.meta.organization_id}
    AND project_id IN ({', '.join(str(pid) for pid in request.meta.project_ids)})
    AND timestamp BETWEEN fromUnixTimestamp({request.meta.start_timestamp.seconds}) AND fromUnixTimestamp({request.meta.end_timestamp.seconds})

    UNION ALL

    SELECT DISTINCT attr_key, 'num' as type, timestamp
    FROM {num_data_source.get_table_name()}
    WHERE organization_id={request.meta.organization_id}
    AND project_id IN ({', '.join(str(pid) for pid in request.meta.project_ids)})
)
ORDER BY attr_key
LIMIT {request.limit} OFFSET {request.offset}
"""
    print(query, file=open("/tmp/derp", "w+"))

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
