from typing import Type

from sentry_protos.snuba.v1.endpoint_delete_trace_items_pb2 import (
    DeleteTraceItemsRequest,
    DeleteTraceItemsResponse,
)

from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


class EndpointDeleteTraceItems(RPCEndpoint[DeleteTraceItemsRequest, DeleteTraceItemsResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[DeleteTraceItemsRequest]:
        return DeleteTraceItemsRequest

    @classmethod
    def response_class(cls) -> Type[DeleteTraceItemsResponse]:
        return DeleteTraceItemsResponse

    def _execute(self, request: DeleteTraceItemsRequest) -> DeleteTraceItemsResponse:
        if len(request.trace_ids) < 1:
            raise BadSnubaRPCRequestException("trace_id is required for deleting a trace.")

        response = DeleteTraceItemsResponse()
        response.matching_items_count = 0
        response.meta.request_id = request.meta.request_id

        return response
