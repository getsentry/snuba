import uuid
from typing import Sequence

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.rpc.common.common import (
    add_existence_check_to_subscriptable_references,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import setup_trace_query_settings
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.R_eap_spans.resolver_trace_item_table import (
    ResolverTraceItemTableEAPSpans,
)
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.attribute_key_to_expression import (
    attribute_key_to_expression,
)

_DEFAULT_ROW_LIMIT = 10_000


def _convert_order_by(
    order_by: Sequence[TraceItemTableRequest.OrderBy],
) -> Sequence[OrderBy]:
    res: list[OrderBy] = []
    for x in order_by:
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC
        if x.column.HasField("key"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=attribute_key_to_expression(x.column.key),
                )
            )
        else:
            raise BadSnubaRPCRequestException(
                "order_by attribute is not a column (aggregation not supported for logs)"
            )
    return res


def _build_query(request: TraceItemTableRequest) -> Query:
    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for col in request.columns:
        if col.HasField("key"):
            key_col = attribute_key_to_expression(col.key)
            # The key_col expression alias may differ from the column label. That is okay
            # the attribute key name is used in the groupby, the column label is just the name of
            # the returned attribute value
            selected_columns.append(
                SelectedExpression(name=col.label, expression=key_col)
            )
        else:
            raise BadSnubaRPCRequestException(
                "requested attribute is not a column (aggregation and formulanot supported for logs)"
            )

    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            f.equals(column("item_type"), TraceItemType.TRACE_ITEM_TYPE_LOG),
            trace_item_filters_to_expression(
                request.filter, attribute_key_to_expression
            ),
        ),
        order_by=_convert_order_by(request.order_by),
        groupby=[
            attribute_key_to_expression(attr_key) for attr_key in request.group_by
        ],
        # Only support offset page tokens for now
        offset=request.page_token.offset,
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
    )
    treeify_or_and_conditions(res)
    add_existence_check_to_subscriptable_references(res)
    return res


def _build_snuba_request(request: TraceItemTableRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="ourlogs",
            feature="ourlogs",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="ourlog_trace_item_table",
        ),
    )


class ResolverTraceItemTableOurlogs(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        """
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        """
        res = ResolverTraceItemTableEAPSpans().resolve(in_msg)
        # option 2 at this point convert the timestamp alias
        return res
