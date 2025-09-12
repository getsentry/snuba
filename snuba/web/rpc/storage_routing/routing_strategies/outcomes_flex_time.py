import uuid
from typing import cast

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp as TimestampProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

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
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    timestamp_in_range_condition,
    treeify_or_and_conditions,
)
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
    RoutingDecision,
    TimeWindow,
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


class OutcomesFlexTimeRoutingStrategy(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="max_items_to_query",
                description="Maximum number of items to query before adjusting time window",
                value_type=int,
                default=100_000_000,
            ),
            Configuration(
                name="time_window_reduction_factor",
                description="Factor by which to reduce time window when too many items",
                value_type=float,
                default=0.5,
            ),
            Configuration(
                name="min_time_window_seconds",
                description="Minimum time window in seconds",
                value_type=int,
                default=300,  # 5 minutes
            ),
        ]

    def _get_max_items_to_query(self) -> int:
        default = 100_000_000
        return (
            state.get_int_config(
                f"{self.class_name()}.max_items_to_query",
                default,
            )
            or default
        )

    def _get_time_window_reduction_factor(self) -> float:
        default = 0.5
        return (
            state.get_float_config(
                f"{self.class_name()}.time_window_reduction_factor",
                default,
            )
            or default
        )

    def _get_min_time_window_seconds(self) -> int:
        default = 300  # 5 minutes
        return (
            state.get_int_config(
                f"{self.class_name()}.min_time_window_seconds",
                default,
            )
            or default
        )

    def get_ingested_items_for_timerange(
        self, routing_context: RoutingContext, start_ts: int, end_ts: int
    ) -> int:
        """Get the number of ingested items for a specific time range."""
        in_msg_meta = extract_message_meta(routing_context.in_msg)
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
                timestamp_in_range_condition(start_ts, end_ts),
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
            original_body=MessageToDict(routing_context.in_msg),
            query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=AttributionInfo(
                referrer=in_msg_meta.referrer,
                team="eap",
                feature="eap",
                tenant_ids={
                    "organization_id": in_msg_meta.organization_id,
                    "referrer": "eap.route_outcomes_flex_time",
                },
                app_id=AppID("eap"),
                parent_api="eap.route_outcomes_flex_time",
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

    def _adjust_time_window(self, routing_context: RoutingContext) -> TimeWindow | None:
        """Adjust the time window to ensure we don't exceed MAX_ITEMS_TO_QUERY."""
        in_msg_meta = extract_message_meta(routing_context.in_msg)
        original_start_ts = in_msg_meta.start_timestamp.seconds
        original_end_ts = in_msg_meta.end_timestamp.seconds

        max_items = self._get_max_items_to_query()
        reduction_factor = self._get_time_window_reduction_factor()
        min_window_seconds = self._get_min_time_window_seconds()

        current_start_ts = original_start_ts
        current_end_ts = original_end_ts

        # Track iterations to prevent infinite loops
        max_iterations = 10
        iteration = 0

        while iteration < max_iterations:
            # Get the current window size
            current_window_seconds = current_end_ts - current_start_ts

            # If we've reached the minimum window size, stop
            if current_window_seconds <= min_window_seconds:
                routing_context.extra_info["reached_min_time_window"] = True
                break

            # Query outcomes for current time window
            ingested_items = self.get_ingested_items_for_timerange(
                routing_context, current_start_ts, current_end_ts
            )

            routing_context.extra_info[f"iteration_{iteration}_items"] = ingested_items
            routing_context.extra_info[f"iteration_{iteration}_window_seconds"] = (
                current_window_seconds
            )

            # If we're under the limit, we're done
            if ingested_items <= max_items:
                break

            # Reduce the time window
            new_window_seconds = max(
                int(current_window_seconds * reduction_factor), min_window_seconds
            )

            # Adjust the time window by moving the start time forward
            # Keep the end time the same and move start time forward
            current_start_ts = current_end_ts - new_window_seconds

            iteration += 1

        routing_context.extra_info["time_window_iterations"] = iteration
        routing_context.extra_info["final_start_ts"] = current_start_ts
        routing_context.extra_info["final_end_ts"] = current_end_ts
        routing_context.extra_info["original_window_seconds"] = original_end_ts - original_start_ts
        routing_context.extra_info["final_window_seconds"] = current_end_ts - current_start_ts

        # If the time window was adjusted, return the new window
        if current_start_ts != original_start_ts or current_end_ts != original_end_ts:
            start_timestamp_proto = TimestampProto()
            start_timestamp_proto.seconds = current_start_ts

            end_timestamp_proto = TimestampProto()
            end_timestamp_proto.seconds = current_end_ts

            return TimeWindow(
                start_timesstamp=start_timestamp_proto,  # Note: typo in original class
                end_timestamp=end_timestamp_proto,
            )

        return None

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        routing_decision = RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_1,  # Always TIER_1 as requested
            clickhouse_settings={},
            can_run=True,
        )

        in_msg_meta = extract_message_meta(routing_decision.routing_context.in_msg)

        sentry_sdk.update_current_span(
            attributes={
                "strategy": "outcomes_flex_time",
                "tier": "TIER_1",
            }
        )

        # Always route to highest accuracy mode if requested
        if self._is_highest_accuracy_mode(in_msg_meta):
            routing_decision.routing_context.extra_info["highest_accuracy_mode"] = True
            return routing_decision

        # Check if we need to handle time window adjustment for unknown item types
        if (
            in_msg_meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED
            and in_msg_meta.trace_item_type not in _ITEM_TYPE_TO_OUTCOME
        ):
            routing_decision.routing_context.extra_info["unknown_item_type"] = True
            return routing_decision

        # Adjust time window based on outcomes
        adjusted_time_window = self._adjust_time_window(routing_context)
        if adjusted_time_window:
            routing_decision.time_window = adjusted_time_window
            routing_decision.routing_context.extra_info["time_window_adjusted"] = True

        return routing_decision
