from typing import Type

from sentry_protos.snuba.v1.endpoint_delete_trace_pb2 import (
    DeleteTraceRequest,
    DeleteTraceResponse,
)

from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


class EndpointDeleteTrace(RPCEndpoint[DeleteTraceRequest, DeleteTraceResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[DeleteTraceRequest]:
        return DeleteTraceRequest

    @classmethod
    def response_class(cls) -> Type[DeleteTraceResponse]:
        return DeleteTraceResponse

    def _execute(self, request: DeleteTraceRequest) -> DeleteTraceResponse:
        if not request.trace_id:
            raise BadSnubaRPCRequestException("trace_id is required for deleting a trace.")

        response = DeleteTraceResponse()
        response.matching_items_count = 0
        response.meta.request_id = request.meta.request_id

        return response
