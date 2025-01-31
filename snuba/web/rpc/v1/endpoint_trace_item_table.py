import uuid
from typing import Type

from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc import RPCEndpoint, TraceItemDataResolver
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.visitors.sparse_aggregate_attribute_transformer import (
    SparseAggregateAttributeTransformer,
)

_GROUP_BY_DISALLOWED_COLUMNS = ["timestamp"]


def _apply_labels_to_columns(in_msg: TraceItemTableRequest) -> TraceItemTableRequest:
    def _apply_label_to_column(column: Column) -> None:
        if column.label != "" and column.label is not None:
            return

        if column.HasField("key"):
            column.label = column.key.name

        elif column.HasField("aggregation"):
            column.label = column.aggregation.label

    for column in in_msg.columns:
        _apply_label_to_column(column)

    for order_by in in_msg.order_by:
        _apply_label_to_column(order_by.column)

    return in_msg


def _validate_select_and_groupby(in_msg: TraceItemTableRequest) -> None:
    non_aggregted_columns = set(
        [c.key.name for c in in_msg.columns if c.HasField("key")]
    )
    grouped_by_columns = set([c.name for c in in_msg.group_by])
    aggregation_present = any([c for c in in_msg.columns if c.HasField("aggregation")])
    if non_aggregted_columns != grouped_by_columns and aggregation_present:
        raise BadSnubaRPCRequestException(
            f"Non aggregated columns should be in group_by. non_aggregated_columns: {non_aggregted_columns}, grouped_by_columns: {grouped_by_columns}"
        )

    if not aggregation_present and grouped_by_columns:
        raise BadSnubaRPCRequestException(
            "Aggregation is required when including group_by columns"
        )

    disallowed_group_by_columns = [
        c.name for c in in_msg.group_by if c.name in _GROUP_BY_DISALLOWED_COLUMNS
    ]
    if disallowed_group_by_columns:
        raise BadSnubaRPCRequestException(
            f"Columns {', '.join(disallowed_group_by_columns)} are not permitted in group_by. The following columns are not allowed: {', '.join(_GROUP_BY_DISALLOWED_COLUMNS)}"
        )


def _validate_order_by(in_msg: TraceItemTableRequest) -> None:
    order_by_cols = set([ob.column.label for ob in in_msg.order_by])
    selected_columns = set([c.label for c in in_msg.columns])
    if not order_by_cols.issubset(selected_columns):
        raise BadSnubaRPCRequestException(
            f"Ordered by columns {order_by_cols} not selected: {selected_columns}"
        )


def _transform_request(request: TraceItemTableRequest) -> TraceItemTableRequest:
    """
    This function is for initial processing and transformation of the request after recieving it.
    It is similar to the query processor step of the snql pipeline.
    """
    return SparseAggregateAttributeTransformer(request).transform()


class EndpointTraceItemTable(
    RPCEndpoint[TraceItemTableRequest, TraceItemTableResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemTableRequest]:
        return TraceItemTableRequest

    def get_resolver(
        self, trace_item_type: TraceItemType.ValueType
    ) -> TraceItemDataResolver[TraceItemTableRequest, TraceItemTableResponse]:
        return ResolverTraceItemTable.get_from_trace_item_type(trace_item_type)(
            timer=self._timer, metrics_backend=self._metrics_backend
        )

    @classmethod
    def response_class(cls) -> Type[TraceItemTableResponse]:
        return TraceItemTableResponse

    def _execute(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        in_msg = _apply_labels_to_columns(in_msg)
        _validate_select_and_groupby(in_msg)
        _validate_order_by(in_msg)

        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        if in_msg.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
            raise BadSnubaRPCRequestException(
                "This endpoint requires meta.trace_item_type to be set (are you requesting spans? logs?)"
            )

        in_msg = _transform_request(in_msg)

        resolver = self.get_resolver(in_msg.meta.trace_item_type)
        return resolver.resolve(in_msg)
