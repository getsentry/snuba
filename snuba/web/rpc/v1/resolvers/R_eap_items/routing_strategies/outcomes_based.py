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
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing import (
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
        return cast(int, res.result.get("data", [{}])[0].get("num_items", 0))

    def _get_max_items_before_downsampling(self) -> int:
        default = 1_000_000_000
        return (
            state.get_int_config(
                f"{self.__class__.__name__}.max_items_before_downsampling",
                default,
            )
            or default
        )

    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        # SELECT category, outcome, sum(quantity),  FROM outcomes_hourly_dist WHERE timestamp > now() - 3600 AND org_id=1 AND project_id=1 AND outcome=0 AND category=16 GROUP BY category, outcome LIMIT 100
        # Check if time range is less than 6 hours
        start_ts = routing_context.in_msg.meta.start_timestamp.seconds
        end_ts = routing_context.in_msg.meta.end_timestamp.seconds
        time_range_secs = end_ts - start_ts
        if time_range_secs < 3600 * 4:
            routing_context.extra_info["small_time_range"] = True
            return Tier.TIER_1, {}

        ingested_items = self.get_ingested_items_for_timerange(routing_context)
        routing_context.extra_info["ingested_items"] = ingested_items
        if ingested_items > self._get_max_items_before_downsampling():
            return Tier.TIER_8, {}
        return Tier.TIER_1, {}

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass
