import uuid
from collections import OrderedDict
from typing import Any, Dict, Iterable, Tuple

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    AttributeDistribution,
    AttributeDistributions,
    AttributeDistributionsRequest,
    TraceItemStatsRequest,
    TraceItemStatsResponse,
    TraceItemStatsResult,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import arrayJoin, column, count, if_cond, literal, tupleElement
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.constants import ATTRIBUTE_BUCKETS_EAP_ITEMS
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers import ResolverTraceItemStats
from snuba.web.rpc.v1.resolvers.R_eap_items.heatmap_builder import HeatmapBuilder

_DEFAULT_ROW_LIMIT = 10_000

MAX_BUCKETS = 100
DEFAULT_BUCKETS = 10

COUNT_LABEL = "count()"

EAP_ITEMS_ENTITY = Entity(
    key=EntityKey("eap_items"),
    schema=get_entity(EntityKey("eap_items")).get_data_model(),
    sample=None,
)


def _transform_attr_distribution_results(
    results: Iterable[Dict[str, Any]],
    request_meta: RequestMeta,
) -> Iterable[AttributeDistribution]:
    # Maintain the order of keys, so it is in descending order
    # of most prevelant key-value pair.
    res: OrderedDict[Tuple[str, str], AttributeDistribution] = OrderedDict()

    for row in results:
        attr_key = row["attr_key"]
        attr_value = row["attr_value"]
        default = AttributeDistribution(
            attribute_name=attr_key,
        )
        res.setdefault((attr_key, COUNT_LABEL), default).buckets.append(
            AttributeDistribution.Bucket(label=attr_value, value=row[COUNT_LABEL])
        )

    return list(res.values())


def _build_snuba_request(
    request: TraceItemStatsRequest, query: Query, routing_decision: RoutingDecision
) -> SnubaRequest:
    query_settings = setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    routing_decision.strategy.merge_clickhouse_settings(routing_decision, query_settings)
    query_settings.set_sampling_tier(routing_decision.tier)

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_attribute_stats",
        ),
    )


def _grab_specific_attributes_query(attributes: Iterable[AttributeKey]) -> Expression:
    """
    returns an experssion that selects only the attributes in the allow list. each attribute will be its own row,
    and it will be a tuple like: (key, value)
    """
    # sql select: if the attribute is in attributes_string, return [(key,value)] else return []
    individual_attribute_select = []
    for attribute in attributes:
        attribute_map = column("attributes_string")
        individual_attribute_select.append(
            if_cond(
                f.mapContains(attribute_map, attribute.name),
                f.array(
                    f.tuple(
                        literal(attribute.name),
                        f.arrayElement(
                            attribute_map,
                            literal(attribute.name),
                        ),
                    )
                ),
                f.array(),
            )
        )
    # sql select: an array of [(key,val), (key,val), ...] containing all the requested attributes
    # the empty arrays are gone now
    concat = f.arrayConcat(*individual_attribute_select)
    # now each tuple will be its own row, (key,val)
    kv = arrayJoin(
        "kv",
        concat,
    )
    return kv


def _grab_all_attributes_query() -> Expression:
    """
    returns an experssion that selects all attributes. each attribute will be its own row, and it will be a tuple like: (key, value)
    """
    concat_attr_maps = FunctionCall(
        alias="attr_str_concat",
        function_name="mapConcat",
        parameters=tuple(
            column(f"attributes_string_{i}") for i in range(ATTRIBUTE_BUCKETS_EAP_ITEMS)
        ),
    )
    kv = arrayJoin(
        "kv",
        concat_attr_maps,
    )
    return kv


def _build_attr_distribution_query(
    in_msg: TraceItemStatsRequest, distributions_params: AttributeDistributionsRequest
) -> Query:
    # kv is a column that contains all attributes in the form of a tuple (key, value)
    # each attribute will be its own row
    if len(distributions_params.attributes) > 0:
        kv = _grab_specific_attributes_query(distributions_params.attributes)
    else:
        kv = _grab_all_attributes_query()

    attrs_string_keys = tupleElement(
        "attr_key",
        column("kv"),
        literal(1),  # index of the key in the tuple
    )
    attrs_string_values = tupleElement(
        "attr_value",
        column("kv"),
        literal(2),  # index of the value in the tuple
    )

    selected_columns = [
        SelectedExpression(
            name="kv",
            expression=kv,
        ),
        SelectedExpression(
            name="attr_key",
            expression=attrs_string_keys,
        ),
        SelectedExpression(
            name="attr_value",
            expression=attrs_string_values,
        ),
        SelectedExpression(
            name=COUNT_LABEL,
            expression=count(alias="_count"),
        ),
    ]

    trace_item_filters_expression = trace_item_filters_to_expression(
        in_msg.filter,
        (attribute_key_to_expression),
    )
    item_type_filter = f.equals(column("item_type"), in_msg.meta.trace_item_type)
    query = Query(
        from_clause=EAP_ITEMS_ENTITY,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            in_msg.meta,
            trace_item_filters_expression,
            item_type_filter,
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.DESC,
                expression=count(),
            ),
        ],
        groupby=[
            column("kv"),
        ],
        limitby=LimitBy(
            limit=(
                distributions_params.max_buckets
                if distributions_params.max_buckets > 0
                else DEFAULT_BUCKETS
            ),
            columns=[column("attr_key")],
        ),
        limit=(
            distributions_params.max_attributes
            if distributions_params.max_attributes > 0
            else _DEFAULT_ROW_LIMIT
        ),
    )

    return query


class ResolverTraceItemStatsEAPItems(ResolverTraceItemStats):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED

    def resolve(
        self,
        in_msg: TraceItemStatsRequest,
        routing_decision: RoutingDecision,
    ) -> TraceItemStatsResponse:
        results = []
        for requested_type in in_msg.stats_types:
            result = TraceItemStatsResult()
            if requested_type.HasField("attribute_distributions"):
                if requested_type.attribute_distributions.max_buckets > MAX_BUCKETS:
                    raise BadSnubaRPCRequestException(f"Max allowed buckets is {MAX_BUCKETS}.")

                query = _build_attr_distribution_query(
                    in_msg, requested_type.attribute_distributions
                )
                treeify_or_and_conditions(query)

                snuba_request = _build_snuba_request(in_msg, query, routing_decision)
                query_res = run_query(
                    dataset=PluggableDataset(name="eap", all_entities=[]),
                    request=snuba_request,
                    timer=self._timer,
                )
                routing_decision.routing_context.query_result = query_res

                attributes = _transform_attr_distribution_results(
                    query_res.result.get("data", []), in_msg.meta
                )
                result.attribute_distributions.CopyFrom(
                    AttributeDistributions(attributes=attributes)
                )
            elif requested_type.HasField("heatmap"):
                res_heatmap = HeatmapBuilder(
                    heatmap=requested_type.heatmap,
                    in_msg=in_msg,
                    routing_decision=routing_decision,
                    timer=self._timer,
                    max_buckets=MAX_BUCKETS,
                ).build()
                result.heatmap.CopyFrom(res_heatmap)
            else:
                raise BadSnubaRPCRequestException(
                    f"Invalid stats type {requested_type.WhichOneof('type')}"
                )

            results.append(result)

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [self._timer],
        )

        return TraceItemStatsResponse(
            results=results,
            meta=response_meta,
        )
