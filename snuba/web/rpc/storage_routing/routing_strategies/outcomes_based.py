import uuid
from typing import cast

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Expression
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, literal, literals_array
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    timestamp_in_range_condition,
    treeify_or_and_conditions,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
    RoutingDecision,
)


# TODO import these from sentry-relay
class OutcomeCategory:
    SPAN_INDEXED = 16
    LOG_ITEM = 23


class Outcome:
    ACCEPTED = 0


_ITEM_TYPE_TO_OUTCOME = {
    TraceItemType.TRACE_ITEM_TYPE_SPAN: OutcomeCategory.SPAN_INDEXED,
    TraceItemType.TRACE_ITEM_TYPE_LOG: OutcomeCategory.LOG_ITEM,
}


def project_id_and_org_conditions(meta: RequestMeta) -> Expression:
    return and_cond(
        in_cond(
            column("project_id"),
            literals_array(
                alias=None,
                literals=[literal(pid) for pid in meta.project_ids],
            ),
        ),
        f.equals(column("org_id"), meta.organization_id),
    )


class OutcomesBasedRoutingStrategy(BaseRoutingStrategy):
    def get_ingested_items_for_timerange(self, routing_context: RoutingContext) -> int:
        in_msg_meta = routing_context.in_msg.time_series_request.meta if isinstance(routing_context.in_msg, CreateSubscriptionRequest) else routing_context.in_msg.meta  # type: ignore
        entity = Entity(
            key=EntityKey("outcomes"),
            schema=get_entity(EntityKey("outcomes")).get_data_model(),
            sample=None,
        )
        query = Query(
            from_clause=entity,
            selected_columns=[
                SelectedExpression(
                    name="num_items",
                    expression=f.sum(column("quantity"), alias="num_items"),
                )
            ],
            condition=and_cond(
                project_id_and_org_conditions(in_msg_meta),
                timestamp_in_range_condition(
                    in_msg_meta.start_timestamp.seconds,
                    in_msg_meta.end_timestamp.seconds,
                ),
                f.equals(column("outcome"), Outcome.ACCEPTED),
                f.equals(
                    column("category"),
                    _ITEM_TYPE_TO_OUTCOME.get(
                        in_msg_meta.trace_item_type,
                        OutcomeCategory.SPAN_INDEXED,
                    ),
                ),
            ),
        )
        snuba_request = SnubaRequest(
            id=uuid.uuid4(),
            original_body=MessageToDict(routing_context.in_msg),  # type: ignore
            query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=AttributionInfo(
                referrer=in_msg_meta.referrer,
                team="eap",
                feature="eap",
                tenant_ids={
                    "organization_id": in_msg_meta.organization_id,
                    "referrer": "eap.route_outcomes",
                },
                app_id=AppID("eap"),
                parent_api="eap.route_outcomes",
            ),
        )
        treeify_or_and_conditions(query)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=routing_context.timer,
        )
        routing_context.extra_info["estimation_sql"] = res.extra.get("sql", "")
        return cast(int, res.result.get("data", [{}])[0].get("num_items", 0))

    def _get_max_items_before_downsampling(self) -> int:
        default = 1_000_000_000
        return (
            state.get_int_config(
                f"{self.config_key()}.max_items_before_downsampling",
                default,
            )
            or default
        )

    def _get_min_timerange_to_query_outcomes(self) -> int:
        default = 3600 * 4
        return (
            state.get_int_config(
                f"{self.config_key()}.min_timerange_to_query_outcomes",
                default,
            )
            or default
        )

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        print("a")
        routing_decision = RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_1,
            clickhouse_settings={},
            can_run=True,
        )
        print("b")
        if self._is_highest_accuracy_mode(routing_context):
            print("c")
            return routing_decision
        # if we're querying a short enough timeframe, don't bother estimating, route to tier 1 and call it a day
        in_msg_meta = routing_decision.routing_context.in_msg.time_series_request.meta if isinstance(routing_decision.routing_context.in_msg, CreateSubscriptionRequest) else routing_decision.routing_context.in_msg.meta  # type: ignore
        print("d")
        start_ts = in_msg_meta.start_timestamp.seconds
        end_ts = in_msg_meta.end_timestamp.seconds
        time_range_secs = end_ts - start_ts
        min_timerange_to_query_outcomes = self._get_min_timerange_to_query_outcomes()
        print("e")
        if time_range_secs < min_timerange_to_query_outcomes:
            print("f")
            routing_decision.routing_context.extra_info[
                "min_timerange_to_query_outcomes"
            ] = min_timerange_to_query_outcomes
            routing_decision.routing_context.extra_info[
                "time_range_secs"
            ] = time_range_secs
            return routing_decision

        # see how many items this combo of orgs/projects has actually ingested for the timerange,
        # downsample if it's too many
        print("g")
        ingested_items = self.get_ingested_items_for_timerange(
            routing_decision.routing_context
        )
        print("h")
        routing_decision.routing_context.extra_info["ingested_items"] = ingested_items
        print("i")
        max_items_before_downsampling = self._get_max_items_before_downsampling()
        print("j")
        routing_decision.routing_context.extra_info[
            "max_items_before_downsampling"
        ] = max_items_before_downsampling
        print("k")
        if (
            ingested_items > max_items_before_downsampling
            and ingested_items <= max_items_before_downsampling * 10
        ):
            print("l")
            routing_decision.tier = Tier.TIER_8
        elif (
            ingested_items > max_items_before_downsampling * 10
            and ingested_items <= max_items_before_downsampling * 100
        ):
            print("m")
            routing_decision.tier = Tier.TIER_64
        elif ingested_items > max_items_before_downsampling * 100:
            print("n")
            routing_decision.tier = Tier.TIER_512
        print("o")
        return routing_decision
