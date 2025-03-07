import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    ExistsFilter,
    TraceItemFilter,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.reader import Row
from snuba.request import Request as SnubaRequest
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    base_conditions_and,
    convert_filter_offset,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import extract_response_meta
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.proto_visitor import ProtoVisitor, TraceItemFilterWrapper

# max value the user can provide for 'limit' in their request
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
            parent_api=EndpointTraceItemAttributeNames.config_key(),
        ),
    )


class AttributeKeyCollector(ProtoVisitor):
    def __init__(self):
        self.keys = set()

    def visit_TraceItemFilterWrapper(
        self, trace_item_filter_wrapper: TraceItemFilterWrapper
    ):
        trace_item_filter = trace_item_filter_wrapper.underlying_proto
        if isinstance(trace_item_filter, (ExistsFilter, ComparisonFilter)):
            self.keys.add(trace_item_filter.key.name)


def convert_to_attributes(
    query_res: QueryResult, attribute_type: AttributeKey.Type.ValueType
) -> list[TraceItemAttributeNamesResponse.Attribute]:
    def t(row: Row) -> TraceItemAttributeNamesResponse.Attribute:
        # our query to snuba only selected 1 column, attr_key
        # so the result should only have 1 item per row
        vals = row.values()
        assert len(vals) == 1
        attr_name = list(vals)[0]
        return TraceItemAttributeNamesResponse.Attribute(
            name=attr_name, type=attribute_type
        )

    return list(map(t, query_res.result["data"]))


def get_co_occurring_attributes(request: TraceItemAttributeNamesRequest) -> QueryResult:
    """Query:

    SELECT DISTINCT(attr_key) FROM (
        SELECT arrayJoin(
            arrayFilter(
                attr -> NOT in(attr, ['allocation_policy.is_throttled']), attributes_string)
            ) AS attr_key
        FROM eap_trace_item_attrs_dist
        WHERE
        hasAll(attributes_string_hash, [cityHash64('allocation_policy.is_throttled')])
        AND project_id IN [1]
        AND organization_id = 1
        AND item_type = 1 -- item type 1 is spans
        AND date >= toDate('2025-02-12') AND date < toDate('2025-02-14')
        LIMIT 10000
    ) LIMIT 1000
    """
    # get all attribute keys from the filter
    collector = AttributeKeyCollector()
    TraceItemFilterWrapper(request.intersecting_attributes_filter).accept(collector)
    attribute_keys_to_search = collector.keys

    # create the composite query, add a time limit with query settings

    # construct result

    # return result


class EndpointTraceItemAttributeNames(
    RPCEndpoint[TraceItemAttributeNamesRequest, TraceItemAttributeNamesResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemAttributeNamesRequest]:
        return TraceItemAttributeNamesRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemAttributeNamesResponse]:
        return TraceItemAttributeNamesResponse

    def _build_response(
        self,
        req: TraceItemAttributeNamesRequest,
        res: QueryResult,
    ) -> TraceItemAttributeNamesResponse:
        attributes = convert_to_attributes(res, req.type)
        page_token = (
            PageToken(offset=req.page_token.offset + len(attributes))
            if req.page_token.HasField("offset") or len(attributes) == 0
            else PageToken(
                filter_offset=TraceItemFilter(
                    comparison_filter=ComparisonFilter(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="attr_key"
                        ),
                        op=ComparisonFilter.OP_GREATER_THAN,
                        value=AttributeValue(val_str=attributes[-1].name),
                    )
                )
            )
        )
        return TraceItemAttributeNamesResponse(
            attributes=attributes,
            page_token=page_token,
            meta=extract_response_meta(
                req.meta.request_id, req.meta.debug, [res], [self._timer]
            ),
        )

    def _execute(
        self, req: TraceItemAttributeNamesRequest
    ) -> TraceItemAttributeNamesResponse:
        if not req.meta.request_id:
            req.meta.request_id = str(uuid.uuid4())
        if req.HasField("intersecting_attributes_filter"):
            res = get_co_occurring_attributes(req)
        else:
            snuba_request = convert_to_snuba_request(req)
            res = run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=snuba_request,
                timer=self._timer,
            )
        return self._build_response(req, res)
