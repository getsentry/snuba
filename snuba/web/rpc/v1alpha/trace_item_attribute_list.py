from typing import List, Optional, Type

from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesRequest as TraceItemAttributesRequestProto,
)
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesResponse,
)
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import truncate_request_meta_to_day
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


class TraceItemAttributesRequest(
    RPCEndpoint[TraceItemAttributesRequestProto, TraceItemAttributesResponse]
):
    @classmethod
    def request_class(cls) -> Type[TraceItemAttributesRequestProto]:
        return TraceItemAttributesRequestProto

    @classmethod
    def version(cls) -> str:
        return "v1alpha"

    def _execute(
        self, in_msg: TraceItemAttributesRequestProto
    ) -> TraceItemAttributesResponse:
        if in_msg.type == AttributeKey.Type.TYPE_STRING:
            storage = get_storage(StorageKey("spans_str_attrs"))
        elif in_msg.type == AttributeKey.Type.TYPE_FLOAT:
            storage = get_storage(StorageKey("spans_num_attrs"))
        else:
            return TraceItemAttributesResponse(tags=[])

        data_source = storage.get_schema().get_data_source()
        assert isinstance(data_source, TableSource)

        if in_msg.limit > 1000:
            raise BadSnubaRPCRequestException("Limit can be at most 1000")

        truncate_request_meta_to_day(in_msg.meta)

        query = f"""
    SELECT DISTINCT attr_key, timestamp
    FROM {data_source.get_table_name()}
    WHERE organization_id={in_msg.meta.organization_id}
    AND project_id IN ({', '.join(str(pid) for pid in in_msg.meta.project_ids)})
    AND timestamp BETWEEN fromUnixTimestamp({in_msg.meta.start_timestamp.seconds}) AND fromUnixTimestamp({in_msg.meta.end_timestamp.seconds})
    ORDER BY attr_key
    LIMIT {in_msg.limit} OFFSET {in_msg.offset}
    """

        cluster = storage.get_cluster()
        reader = cluster.get_reader()
        result = reader.execute(FormattedQuery([StringNode(query)]))

        tags: List[TraceItemAttributesResponse.Tag] = []
        for row in result.get("data", []):
            tags.append(
                TraceItemAttributesResponse.Tag(
                    name=row["attr_key"],
                    type=in_msg.type,
                )
            )

        return TraceItemAttributesResponse(tags=tags)
