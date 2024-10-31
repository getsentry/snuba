import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesRequest as TraceItemAttributesRequestProto,
)
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    TraceItemAttributesResponse,
)
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1alpha.common import (
    base_conditions_and,
    treeify_or_and_conditions,
    truncate_request_meta_to_day,
)


class TraceItemAttributesRequest(
    RPCEndpoint[TraceItemAttributesRequestProto, TraceItemAttributesResponse]
):
    @classmethod
    def request_class(cls) -> Type[TraceItemAttributesRequestProto]:
        return TraceItemAttributesRequestProto

    @classmethod
    def response_class(cls) -> Type[TraceItemAttributesResponse]:
        return TraceItemAttributesResponse

    @classmethod
    def version(cls) -> str:
        return "v1alpha"

    def _execute(
        self, in_msg: TraceItemAttributesRequestProto
    ) -> TraceItemAttributesResponse:
        return trace_item_attribute_list_query(in_msg, self._timer)


def _build_query(request: TraceItemAttributesRequestProto) -> Query:
    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    if request.type == AttributeKey.Type.TYPE_STRING:
        entity = Entity(
            key=EntityKey("spans_str_attrs"),
            schema=get_entity(EntityKey("spans_str_attrs")).get_data_model(),
            sample=None,
        )
    elif request.type in (
        AttributeKey.Type.TYPE_FLOAT,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_BOOLEAN,
    ):
        entity = Entity(
            key=EntityKey("spans_num_attrs"),
            schema=get_entity(EntityKey("spans_num_attrs")).get_data_model(),
            sample=None,
        )
    else:
        raise BadSnubaRPCRequestException(f"Unknown attribute type: {request.type}")

    truncate_request_meta_to_day(request.meta)

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="attr_key",
                expression=column("attr_key", alias="attr_key"),
            ),
        ],
        condition=base_conditions_and(request.meta),
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC, expression=column("organization_id")
            ),
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_key")),
        ],
        groupby=[
            column("organization_id", alias="organization_id"),
            column("attr_key", alias="attr_key"),
        ],
        limit=request.limit,
        offset=request.offset,
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(
    request: TraceItemAttributesRequestProto,
) -> SnubaRequest:
    return SnubaRequest(
        id=uuid.uuid4(),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="trace_item_attribute_list",
        ),
    )


def trace_item_attribute_list_query(
    request: TraceItemAttributesRequestProto, timer: Timer | None = None
) -> TraceItemAttributesResponse:
    timer = timer or Timer("trace_item_attributes")
    snuba_request = _build_snuba_request(request)
    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=snuba_request,
        timer=timer,
    )
    return TraceItemAttributesResponse(
        tags=[
            TraceItemAttributesResponse.Tag(name=r["attr_key"], type=request.type)
            for r in res.result.get("data", [])
        ]
    )
