import uuid
from typing import Type

from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    TraceItemStatsRequest,
    TraceItemStatsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc import RPCEndpoint, TraceItemDataResolver
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemStats


class EndpointTraceItemStats(
    RPCEndpoint[TraceItemStatsRequest, TraceItemStatsResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemStatsRequest]:
        return TraceItemStatsRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemStatsResponse]:
        return TraceItemStatsResponse

    def get_resolver(
        self, trace_item_type: TraceItemType.ValueType
    ) -> TraceItemDataResolver[TraceItemStatsRequest, TraceItemStatsResponse]:
        return ResolverTraceItemStats.get_from_trace_item_type(trace_item_type)(
            timer=self._timer,
            metrics_backend=self._metrics_backend,
        )

    def _execute(self, in_msg: TraceItemStatsRequest) -> TraceItemStatsResponse:
        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )

        if not in_msg.stats_types:
            raise BadSnubaRPCRequestException("Please specify at least one stats type.")

        if in_msg.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
            raise BadSnubaRPCRequestException(
                "This endpoint requires meta.trace_item_type to be set (are you requesting spans? logs?)"
            )
        resolver = self.get_resolver(in_msg.meta.trace_item_type)
        return resolver.resolve(in_msg)
