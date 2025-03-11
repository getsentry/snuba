import uuid
from datetime import timedelta
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, Storage
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, not_cond
from snuba.query.expressions import Expression, Lambda
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
    project_id_and_org_conditions,
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
        if trace_item_filter.HasField("exists_filter"):
            self.keys.add(trace_item_filter.exists_filter.key.name)
        elif trace_item_filter.HasField("comparison_filter"):
            self.keys.add(trace_item_filter.comparison_filter.key.name)


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


def get_co_occurring_attributes_date_condition(
    request: TraceItemAttributeNamesRequest,
) -> Expression:
    # round the lower timestamp to the previous monday
    lower_ts = request.meta.start_timestamp.ToDatetime().date()
    lower_ts = lower_ts - timedelta(days=(lower_ts.weekday() - 0) % 7)

    # round the upper timestamp to the next monday
    upper_ts = request.meta.end_timestamp.ToDatetime().date()
    upper_ts = upper_ts + timedelta(days=(7 - upper_ts.weekday()) % 7)

    return and_cond(
        f.less(
            column("date"),
            f.toDateTime(upper_ts),
        ),
        f.greaterOrEquals(
            column("date"),
            f.toDateTime(lower_ts),
        ),
    )


def get_co_occurring_attributes(
    request: TraceItemAttributeNamesRequest,
) -> SnubaRequest:
    """Query:

    # this is not accurate anymore due to the query being able to not take a "TYPE" as a required attribute
    # but we still need to return the type so:


        array_func = arrayConcat(arrayMap(x -> ('string', x), attributes_string), arrayMap(x -> ('float', x), attributes_float)) <--

            arrayFilter(
                attr -> NOT in(attr, ['allocation_policy.is_throttled']), array_func)
            ) AS attr_key


    SELECT DISTINCT(attr_key) FROM (
        SELECT arrayJoin(
            arrayFilter(
                -- TODO: add value substring match
                attr -> NOT in(attr.2, ['allocation_policy.is_throttled']), arrayConcat(arrayMap(x -> ('string', x), attributes_string), arrayMap(x -> ('float', x), attributes_float)))
            ) AS attr_key
        FROM eap_item_co_occurring_attrs_1_dist
        WHERE
        hasAll(attribute_keys_hash, [cityHash64('allocation_policy.is_throttled')])
        AND project_id IN [1]
        AND organization_id = 1
        AND item_type = 1 -- item type 1 is spans
        AND date >= toDate('2025-03-03') AND date < toDate('2025-03-12')
        LIMIT 10000
    ) LIMIT 1000
    """
    # get all attribute keys from the filter
    collector = AttributeKeyCollector()
    TraceItemFilterWrapper(request.intersecting_attributes_filter).accept(collector)
    attribute_keys_to_search = collector.keys

    storage = Storage(
        key=StorageKey("eap_item_co_occurring_attrs"),
        schema=get_storage(StorageKey("eap_item_co_occurring_attrs"))
        .get_schema()
        .get_columns(),
        sample=None,
    )

    condition = and_cond(
        and_cond(
            project_id_and_org_conditions(request.meta),
            # timestamp should be converted to start and end of week
            get_co_occurring_attributes_date_condition(request),
        ),
        f.hasAll(
            column("attribute_keys_hash"),
            f.array(*[f.cityHash64(k) for k in attribute_keys_to_search]),
        ),
    )

    if request.meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
        condition = and_cond(
            f.equals(column("item_type"), request.meta.trace_item_type), condition
        )

    # array_func = f.arrayConcat(arrayMap(x -> ('string', x), attributes_string), arrayMap(x -> ('float', x), attributes_float))
    # TODO: don't concat if the user specified a specific type
    array_func = f.arrayConcat(
        f.arrayMap(
            Lambda(None, ("x",), f.tuple("TYPE_STRING", column("x"))),
            column("attributes_string"),
        ),
        f.arrayMap(
            Lambda(None, ("x",), f.tuple("TYPE_DOUBLE", column("x"))),
            column("attributes_float"),
        ),
    )

    attr_filter = not_cond(
        in_cond(column("attr.2"), f.array(*attribute_keys_to_search))
    )
    if request.value_substring_match:
        attr_filter = and_cond(
            attr_filter, f.startsWith(column("attr.2"), request.value_substring_match)
        )

    inner_query = Query(
        from_clause=storage,
        selected_columns=[
            SelectedExpression(
                name="attr_key",
                expression=f.arrayJoin(
                    f.arrayFilter(
                        # TODO: value_substring_match
                        Lambda(None, ("attr",), attr_filter),
                        array_func,
                    ),
                    alias="attr_key",
                ),
            ),
        ],
        condition=condition,
        order_by=[
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_key")),
        ],
        # chosen arbitrarily to be a high number
        limit=10000,
    )

    full_query = CompositeQuery(
        from_clause=inner_query,
        selected_columns=[
            SelectedExpression(
                name="attr_key", expression=f.distinct(column("attr_key"))
            )
        ],
        limit=1000,
    )
    treeify_or_and_conditions(full_query)
    snuba_request = SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=full_query,
        # TODO: Add time limit
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
            parent_api=EndpointTraceItemAttributeNames.config_key(),
        ),
    )
    return snuba_request


def convert_co_occurring_results_to_attributes(
    query_res: QueryResult,
) -> list[TraceItemAttributeNamesResponse.Attribute]:
    def t(row: Row) -> TraceItemAttributeNamesResponse.Attribute:
        # our query to snuba only selected 1 column, attr_key
        # so the result should only have 1 item per row
        vals = row.values()
        assert len(vals) == 1
        attr_type, attr_name = list(vals)[0]
        assert isinstance(attr_type, str)
        return TraceItemAttributeNamesResponse.Attribute(
            name=attr_name, type=getattr(AttributeKey.Type, attr_type)
        )

    return list(map(t, query_res.result.get("data", [])))


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
        self, in_msg: TraceItemAttributeNamesRequest
    ) -> TraceItemAttributeNamesResponse:
        if not in_msg.meta.request_id:
            in_msg.meta.request_id = str(uuid.uuid4())
        if in_msg.HasField("intersecting_attributes_filter"):
            snuba_request = get_co_occurring_attributes(in_msg)
            res = run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=snuba_request,
                timer=self._timer,
            )
            return TraceItemAttributeNamesResponse(
                attributes=convert_co_occurring_results_to_attributes(res),
                meta=extract_response_meta(
                    in_msg.meta.request_id, in_msg.meta.debug, [res], [self._timer]
                ),
            )
        else:
            snuba_request = convert_to_snuba_request(in_msg)
            res = run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=snuba_request,
                timer=self._timer,
            )
        return self._build_response(in_msg, res)
