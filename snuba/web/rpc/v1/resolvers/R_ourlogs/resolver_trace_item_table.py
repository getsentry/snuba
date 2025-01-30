import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Sequence

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc.common.common import base_conditions_and, treeify_or_and_conditions
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.common.trace_item_table import convert_results
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.attribute_key_to_expression import (
    attribute_key_to_expression,
)
from snuba.web.rpc.v1.resolvers.R_ourlogs.common.trace_item_filters_to_expression import (
    trace_item_filters_to_expression,
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
        key=EntityKey("ourlogs"),
        schema=get_entity(EntityKey("ourlogs")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for column in request.columns:
        if column.HasField("key"):
            key_col = attribute_key_to_expression(column.key)
            # The key_col expression alias may differ from the column label. That is okay
            # the attribute key name is used in the groupby, the column label is just the name of
            # the returned attribute value
            selected_columns.append(
                SelectedExpression(name=column.label, expression=key_col)
            )
        else:
            raise BadSnubaRPCRequestException(
                "requested attribute is not a column (aggregation not supported for logs)"
            )

    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(request.filter),
        ),
        order_by=_convert_order_by(request.order_by),
        groupby=[
            attribute_key_to_expression(attr_key) for attr_key in request.group_by
        ],
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
    )
    treeify_or_and_conditions(res)
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


def _get_page_token(
    request: TraceItemTableRequest, response: list[TraceItemColumnValues]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response[0].results)
    return PageToken(offset=request.page_token.offset + num_rows)


class ResolverTraceItemTableOurlogs(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        column_values = convert_results(in_msg, res.result.get("data", []))
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )
        return TraceItemTableResponse(
            column_values=column_values,
            page_token=_get_page_token(in_msg, column_values),
            meta=response_meta,
        )
