import uuid
from typing import cast

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

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
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    ClickhouseQuerySettings,
    RoutingContext,
)


# TODO import these from sentry-relay
class Outcome:
    SPAN_INDEXED = 16
    LOG_ITEM = 23


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
                project_id_and_org_conditions(routing_context.in_msg.meta),
                timestamp_in_range_condition(
                    routing_context.in_msg.meta.start_timestamp.seconds,
                    routing_context.in_msg.meta.end_timestamp.seconds,
                ),
                f.equals(column("outcome"), 0),
                in_cond(
                    column("category"),
                    literals_array(
                        alias=None,
                        literals=[
                            literal(Outcome.SPAN_INDEXED),
                            literal(Outcome.LOG_ITEM),
                        ],
                    ),
                ),
            ),
        )
        snuba_request = SnubaRequest(
            id=uuid.uuid4(),
            original_body=MessageToDict(routing_context.in_msg),
            query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=AttributionInfo(
                referrer=routing_context.in_msg.meta.referrer,
                team="eap",
                feature="eap",
                tenant_ids={
                    "organization_id": routing_context.in_msg.meta.organization_id,
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

    def _get_time_budget_ms(self) -> int:
        default = 8000
        return (
            state.get_int_config(
                f"{self.config_key()}.time_budget_ms",
                default,
            )
            or default
        )

    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        # if we're querying a short enough timeframe, don't bother estimating, route to tier 1 and call it a day
        start_ts = routing_context.in_msg.meta.start_timestamp.seconds
        end_ts = routing_context.in_msg.meta.end_timestamp.seconds
        time_range_secs = end_ts - start_ts
        min_timerange_to_query_outcomes = self._get_min_timerange_to_query_outcomes()
        if time_range_secs < min_timerange_to_query_outcomes:
            routing_context.extra_info[
                "min_timerange_to_query_outcomes"
            ] = min_timerange_to_query_outcomes
            routing_context.extra_info["time_range_secs"] = time_range_secs
            return Tier.TIER_1, {}

        # see how many items this combo of orgs/projects has actually ingested for the timerange,
        # downsample if it's too many
        ingested_items = self.get_ingested_items_for_timerange(routing_context)
        routing_context.extra_info["ingested_items"] = ingested_items
        max_items_before_downsampling = self._get_max_items_before_downsampling()
        routing_context.extra_info[
            "max_items_before_downsampling"
        ] = max_items_before_downsampling
        if ingested_items > max_items_before_downsampling:
            return Tier.TIER_8, {}
        return Tier.TIER_1, {}

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        if not routing_context.query_result:
            return
        profile = routing_context.query_result.result.get("profile", {}) or {}
        if elapsed := profile.get("elapsed"):
            time_budget = self._get_time_budget_ms()
            routing_context.extra_info["time_budget"] = time_budget
            if elapsed > time_budget:
                self._record_value_in_span_and_DD(
                    routing_context=routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_mistake",
                    value=1,
                    tags={"reason": "timeout"},
                )
            elif routing_context.query_settings.get_sampling_tier() != Tier.TIER_1:
                if elapsed < 0.2 * time_budget:
                    self._record_value_in_span_and_DD(
                        routing_context=routing_context,
                        metrics_backend_func=self.metrics.increment,
                        name="routing_mistake",
                        value=1,
                        tags={"reason": "sampled_too_low"},
                    )
            else:
                self._record_value_in_span_and_DD(
                    routing_context=routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_success",
                    value=1,
                    tags={},
                )
