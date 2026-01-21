import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Expression
from snuba.configs.configuration import Configuration
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, literal, literals_array
from snuba.query.logical import Query
from snuba.query.query_settings import OutcomesQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    timestamp_in_range_condition,
    treeify_or_and_conditions,
)
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.routing_strategies.common import (
    ITEM_TYPE_FULL_RETENTION,
    ITEM_TYPE_TO_OUTCOME_CATEGORY,
    Outcome,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
    RoutingDecision,
)


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
    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="some_additional_config",
                description="Placeholder for now",
                value_type=int,
                default=50,
                param_types={"organization_id": int},
            ),
        ]

    def _use_daily(self, in_msg_meta: RequestMeta) -> bool:
        if in_msg_meta.end_timestamp.seconds < in_msg_meta.start_timestamp.seconds:
            return False
        seconds_delta = in_msg_meta.end_timestamp.seconds - in_msg_meta.start_timestamp.seconds
        duration = timedelta(seconds=seconds_delta)
        return duration.days > 90

    def get_ingested_items_for_timerange(self, routing_context: RoutingContext) -> int:
        in_msg_meta = extract_message_meta(routing_context.in_msg)
        entity = Entity(
            key=EntityKey("outcomes"),
            schema=get_entity(EntityKey("outcomes")).get_data_model(),
            sample=None,
        )
        query_settings = (
            OutcomesQuerySettings(use_daily=True)
            if self._use_daily(in_msg_meta)
            else OutcomesQuerySettings()
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
                    column("category"), ITEM_TYPE_TO_OUTCOME_CATEGORY[in_msg_meta.trace_item_type]
                ),
            ),
        )
        snuba_request = SnubaRequest(
            id=uuid.uuid4(),
            original_body=MessageToDict(routing_context.in_msg),
            query=query,
            query_settings=query_settings,
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
                f"{self.class_name()}.max_items_before_downsampling",
                default,
            )
            or default
        )

    def _get_min_timerange_to_query_outcomes(self) -> int:
        default = 3600 * 4
        return (
            state.get_int_config(
                f"{self.class_name()}.min_timerange_to_query_outcomes",
                default,
            )
            or default
        )

    def _update_routing_decision(
        self,
        routing_decision: RoutingDecision,
    ) -> None:
        if not routing_decision.can_run:
            return

        in_msg_meta = extract_message_meta(routing_decision.routing_context.in_msg)

        thirty_one_days_ago_ts = int((datetime.now(tz=UTC) - timedelta(days=31)).timestamp())
        older_than_thirty_days = thirty_one_days_ago_ts > in_msg_meta.start_timestamp.seconds

        if (
            state.get_int_config("enable_long_term_retention_downsampling", 0)
            and older_than_thirty_days
            and in_msg_meta.trace_item_type not in ITEM_TYPE_FULL_RETENTION
        ):
            routing_decision.tier = Tier.TIER_8

        sentry_sdk.update_current_span(
            attributes={
                "downsampling_mode": (
                    "highest_accuracy" if self._is_highest_accuracy_mode(in_msg_meta) else "normal"
                ),
            }
        )
        if self._is_highest_accuracy_mode(in_msg_meta) or (
            # unspecified item type will be assumed as spans when querying
            # for GetTraces, there is no type specified so we assume spans because
            # that is necessary for traces anyways
            # if the type is specified and we don't know its outcome, route to Tier_1
            in_msg_meta.trace_item_type not in ITEM_TYPE_TO_OUTCOME_CATEGORY
        ):
            return

        # if we're querying a short enough timeframe, don't bother estimating, route to tier 1 and call it a day
        start_ts = in_msg_meta.start_timestamp.seconds
        end_ts = in_msg_meta.end_timestamp.seconds
        time_range_secs = end_ts - start_ts
        min_timerange_to_query_outcomes = self._get_min_timerange_to_query_outcomes()
        if time_range_secs < min_timerange_to_query_outcomes:
            routing_decision.routing_context.extra_info["min_timerange_to_query_outcomes"] = (
                min_timerange_to_query_outcomes
            )
            routing_decision.routing_context.extra_info["time_range_secs"] = time_range_secs
            return

        # see how many items this combo of orgs/projects has actually ingested for the timerange,
        # downsample if it's too many
        ingested_items = self.get_ingested_items_for_timerange(routing_decision.routing_context)
        routing_decision.routing_context.extra_info["ingested_items"] = ingested_items
        max_items_before_downsampling = self._get_max_items_before_downsampling()
        routing_decision.routing_context.extra_info["max_items_before_downsampling"] = (
            max_items_before_downsampling
        )
        if (
            ingested_items > max_items_before_downsampling
            and ingested_items <= max_items_before_downsampling * 10
        ):
            routing_decision.tier = Tier.TIER_8
        elif (
            ingested_items > max_items_before_downsampling * 10
            and ingested_items <= max_items_before_downsampling * 100
        ):
            routing_decision.tier = Tier.TIER_64
        elif ingested_items > max_items_before_downsampling * 100:
            routing_decision.tier = Tier.TIER_512

        sentry_sdk.update_current_span(
            attributes={
                "ingested_items": ingested_items,
                "max_items_before_downsampling": max_items_before_downsampling,
                "tier": routing_decision.tier.name,
            }
        )
