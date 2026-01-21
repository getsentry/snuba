import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeValuesRequest,
    TraceItemAttributeValuesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
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
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, LogicalDataSource
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    base_conditions_and,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)


def _build_conditions(request: TraceItemAttributeValuesRequest) -> Expression:
    attribute_key = attribute_key_to_expression(request.key)

    conditions: list[Expression] = [
        f.has(column("attributes_string"), getattr(attribute_key, "key", request.key.name)),
    ]
    if request.meta.trace_item_type:
        conditions.append(f.equals(column("item_type"), request.meta.trace_item_type))

    if request.value_substring_match:
        conditions.append(
            f.like(
                attribute_key,
                f"%{request.value_substring_match}%",
            )
        )

    return base_conditions_and(request.meta, *conditions)


def _build_query(
    request: TraceItemAttributeValuesRequest,
) -> CompositeQuery[LogicalDataSource]:
    """Example query:


    SELECT distinct(attr_value) FROM
    (
        SELECT attributes_string_38['sentry.description'] as attr_value
        FROM eap_items_1_dist
        WHERE
        has(attributes_string_38, cityHash64('sentry.description'))
        AND attributes_string_38['sentry.description'] LIKE '%django.middleware%'
        AND project_id = 1 AND organization_id=1 AND item_type=1
        AND less(timestamp, toDateTime(1741910400))
        AND greaterOrEquals(timestamp, toDateTime(1741651200))
        ORDER BY attr_value
        LIMIT 10000
    ) ORDER BY attr_value LIMIT 1000


    This query will match the first 10000 occurrences of an attribute value and then deduplicate them,
    this gives a large speedup to the query at the cost of ordering and paginating all values
    """
    if request.limit > 10000:
        raise BadSnubaRPCRequestException("Limit can be at most 10000")

    entity_key = EntityKey("eap_items")
    entity = Entity(
        key=entity_key,
        schema=get_entity(entity_key).get_data_model(),
        sample=None,
    )
    attr_value = attribute_key_to_expression(request.key)
    assert attr_value.alias
    inner_query = Query(
        from_clause=entity,
        selected_columns=[SelectedExpression(name=attr_value.alias, expression=attr_value)],
        condition=_build_conditions(request),
        offset=0,
        limit=10000,
    )
    treeify_or_and_conditions(inner_query)
    res = CompositeQuery(
        from_clause=inner_query,
        selected_columns=[
            SelectedExpression(
                name="attr_value",
                expression=f.distinct(column(attr_value.alias, alias="attr_value")),
            ),
        ],
        order_by=[
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_value")),
        ],
        limit=request.limit,
        offset=(request.page_token.offset if request.page_token.HasField("offset") else 0),
    )
    return res


def _build_snuba_request(
    request: TraceItemAttributeValuesRequest,
    routing_decision: RoutingDecision,
) -> SnubaRequest:
    settings = HTTPQuerySettings()
    settings.set_sampling_tier(routing_decision.tier)
    return SnubaRequest(
        id=uuid.uuid4(),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="trace_item_values",
        ),
    )


class AttributeValuesRequest(
    RPCEndpoint[TraceItemAttributeValuesRequest, TraceItemAttributeValuesResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TraceItemAttributeValuesRequest]:
        return TraceItemAttributeValuesRequest

    @classmethod
    def response_class(cls) -> Type[TraceItemAttributeValuesResponse]:
        return TraceItemAttributeValuesResponse

    def _execute(self, in_msg: TraceItemAttributeValuesRequest) -> TraceItemAttributeValuesResponse:
        # if for some reason the item_id is the key, we can just return the value
        # item ids are unique
        if in_msg.key.name == "sentry.item_id" and in_msg.value_substring_match:
            return TraceItemAttributeValuesResponse(
                values=[in_msg.value_substring_match],
                page_token=None,
            )
        in_msg.limit = in_msg.limit or 1000
        snuba_request = _build_snuba_request(in_msg, self.routing_decision)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        values = [r["attr_value"] for r in res.result.get("data", [])]
        if len(values) == 0:
            return TraceItemAttributeValuesResponse(
                values=values,
                page_token=None,
            )
        return TraceItemAttributeValuesResponse(
            values=values,
            page_token=(
                PageToken(offset=in_msg.page_token.offset + len(values))
                if in_msg.page_token.HasField("offset") or len(values) == 0
                else PageToken(
                    filter_offset=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="attr_value"),
                            op=ComparisonFilter.OP_GREATER_THAN,
                            value=AttributeValue(val_str=values[-1]),
                        )
                    )
                )
            ),
        )
