import uuid
from typing import Type

from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc import RPCEndpoint, TraceItemDataResolver
from snuba.web.rpc.common.debug_info import extract_response_meta
from snuba.web.rpc.v1.resolvers import ResolverGetTrace


class EndpointGetTrace(RPCEndpoint[GetTraceRequest, GetTraceResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[GetTraceRequest]:
        return GetTraceRequest

    @classmethod
    def response_class(cls) -> Type[GetTraceResponse]:
        return GetTraceResponse

    def get_resolver(
        self, trace_item_type: TraceItemType.ValueType
    ) -> TraceItemDataResolver[GetTraceRequest, GetTraceResponse]:
        return ResolverGetTrace.get_from_trace_item_type(trace_item_type)(
            timer=self._timer,
            metrics_backend=self._metrics_backend,
        )

    def _execute(self, in_msg: GetTraceRequest) -> GetTraceResponse:
        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )
        responses = [
            self.get_resolver(item.item_type).resolve(in_msg) for item in in_msg.items
        ]
        return GetTraceResponse(
            item_groups=[
                item_group
                for response in responses
                for item_group in response.item_groups
            ],
            meta=response_meta,
            trace_id=in_msg.trace_id,
        )
