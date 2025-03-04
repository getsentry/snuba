from typing import Type

from sentry_protos.snuba.v1.endpoint_trace_item_details_pb2 import (
    TraceItemDetailsRequest,
    TraceItemDetailsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc import RPCEndpoint, TraceItemDataResolver
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemDetails


class EndpointTraceItemDetails(
    RPCEndpoint[TraceItemDetailsRequest, TraceItemDetailsResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemDetailsRequest]:
        return TraceItemDetailsRequest

    def get_resolver(
        self, trace_item_type: TraceItemType.ValueType
    ) -> TraceItemDataResolver[TraceItemDetailsRequest, TraceItemDetailsResponse]:
        return ResolverTraceItemDetails.get_from_trace_item_type(trace_item_type)(
            timer=self._timer, metrics_backend=self._metrics_backend
        )

    @classmethod
    def response_class(cls) -> Type[TraceItemDetailsResponse]:
        return TraceItemDetailsResponse

    def _execute(self, in_msg: TraceItemDetailsRequest) -> TraceItemDetailsResponse:
        if in_msg.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
            raise BadSnubaRPCRequestException(
                "This endpoint requires meta.trace_item_type to be set (are you requesting spans? logs?)"
            )
        resolver = self.get_resolver(in_msg.meta.trace_item_type)
        return resolver.resolve(in_msg)
