import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

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
    base_conditions_and,
    convert_filter_offset,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

MAX_REQUEST_LIMIT = 1000


def convert_to_snuba_request(req: TraceItemAttributeNamesRequest) -> SnubaRequest:
    if req.limit > MAX_REQUEST_LIMIT:
        raise BadSnubaRPCRequestException(
            f"Limit can be at most {MAX_REQUEST_LIMIT}, but was {req.limit}. For larger limits please utilize pagination via the page_token variable in your request."
        )

    if req.type == AttributeKey.Type.TYPE_STRING:
        entity = Entity(
            key=EntityKey("spans_str_attrs"),
            schema=get_entity(EntityKey("spans_str_attrs")).get_data_model(),
            sample=None,
        )
    elif req.type in (
        AttributeKey.Type.TYPE_FLOAT,
        AttributeKey.Type.TYPE_DOUBLE,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_BOOLEAN,
    ):
        entity = Entity(
            key=EntityKey("spans_num_attrs"),
            schema=get_entity(EntityKey("spans_num_attrs")).get_data_model(),
            sample=None,
        )
    else:
        raise BadSnubaRPCRequestException(
            f"Attribute type '{req.type}' is not supported. Supported types are: TYPE_STRING, TYPE_FLOAT, TYPE_INT, TYPE_BOOLEAN"
        )

    condition = (
        base_conditions_and(
            req.meta,
            f.like(column("attr_key"), f"%{req.value_substring_match}%"),
            convert_filter_offset(req.page_token.filter_offset),
        )
        if req.page_token.HasField("filter_offset")
        else base_conditions_and(
            req.meta, f.like(column("attr_key"), f"%{req.value_substring_match}%")
        )
    )

    query = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="attr_key",
                expression=column("attr_key", alias="attr_key"),
            ),
        ],
        condition=condition,
        order_by=[
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_key")),
        ],
        groupby=[
            column("attr_key", alias="attr_key"),
        ],
        limit=req.limit,
        offset=req.page_token.offset,
    )
    treeify_or_and_conditions(query)

    return SnubaRequest(
        id=uuid.UUID(req.meta.request_id),
        original_body=MessageToDict(req),
        query=query,
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            referrer=req.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": req.meta.organization_id,
                "referrer": req.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="EndpointTraceItemAttributeNames__v1",
        ),
    )
