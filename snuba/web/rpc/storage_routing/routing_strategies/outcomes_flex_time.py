import math
import uuid
from typing import cast

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.timestamp_pb2 import Timestamp as TimestampProto
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, RequestMeta

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
from snuba.web.rpc.storage_routing.routing_strategies.common import (
    ITEM_TYPE_TO_OUTCOME_CATEGORY,
    Outcome,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
    RoutingDecision,
    TimeWindow,
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


def _get_request_time_window(routing_context: RoutingContext) -> TimeWindow:
    """Gets the time window of the request, if there is a page token with a start and end timestamp,
    it gets it from there, otherwise, it gets it from the message meta
    """
    meta = extract_message_meta(routing_context.in_msg)
    if routing_context.in_msg.HasField("page_token"):
        page_token: PageToken = getattr(routing_context.in_msg, "page_token")
        if page_token.HasField("filter_offset"):
            time_window = TimeWindow(
                start_timestamp=meta.start_timestamp, end_timestamp=meta.end_timestamp
            )
            if page_token.filter_offset.HasField("and_filter"):
                for filter in page_token.filter_offset.and_filter.filters:
                    if (
                        filter.HasField("comparison_filter")
                        and filter.comparison_filter.key.name == "start_timestamp"
                    ):
                        time_window.start_timestamp = Timestamp(
                            seconds=filter.comparison_filter.value.val_int
                        )
                    if (
                        filter.HasField("comparison_filter")
                        and filter.comparison_filter.key.name == "end_timestamp"
                    ):
                        time_window.end_timestamp = Timestamp(
                            seconds=filter.comparison_filter.value.val_int
                        )
            return time_window
    return TimeWindow(start_timestamp=meta.start_timestamp, end_timestamp=meta.end_timestamp)


class OutcomesFlexTimeRoutingStrategy(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="max_items_to_query",
                description="Maximum number of items to query before adjusting time window",
                value_type=int,
                default=100_000_000,
            ),
        ]

    def get_ingested_items_for_timerange(
        self, routing_context: RoutingContext, time_window: TimeWindow
    ) -> int:
        entity = Entity(
            key=EntityKey("outcomes"),
            schema=get_entity(EntityKey("outcomes")).get_data_model(),
            sample=None,
        )
        in_msg_meta = extract_message_meta(routing_context.in_msg)
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
                    time_window.start_timestamp.seconds,
                    time_window.end_timestamp.seconds,
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

    def _adjust_time_window(self, routing_context: RoutingContext) -> TimeWindow:
        """Adjust the time window to ensure we don't exceed MAX_ITEMS_TO_QUERY."""
        original_time_window = _get_request_time_window(routing_context)
        original_end_ts = original_time_window.end_timestamp.seconds
        original_start_ts = original_time_window.start_timestamp.seconds

        max_items = self.get_config_value("max_items_to_query")

        ingested_items = self.get_ingested_items_for_timerange(
            routing_context, original_time_window
        )
        factor = ingested_items / max_items
        if factor > 1:
            window_length = original_end_ts - original_start_ts

            start_timestamp_proto = TimestampProto(
                seconds=original_end_ts - math.floor((window_length / factor))
            )
            end_timestamp_proto = TimestampProto(seconds=original_end_ts)
            return TimeWindow(start_timestamp_proto, end_timestamp_proto)

        return original_time_window

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        routing_decision = RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_1,  # Always TIER_1
            clickhouse_settings={},
            can_run=True,
        )

        in_msg_meta = extract_message_meta(routing_decision.routing_context.in_msg)

        # if type is unknown, just route to tier 1, no adjustment
        if in_msg_meta.trace_item_type not in ITEM_TYPE_TO_OUTCOME_CATEGORY:
            routing_decision.routing_context.extra_info["unknown_item_type"] = True
            sentry_sdk.capture_message(
                f"Trace Item {in_msg_meta.trace_item_type} does not have an associated outcome"
            )
            return routing_decision

        # Adjust time window based on outcomes
        adjusted_time_window = self._adjust_time_window(routing_context)
        if adjusted_time_window:
            routing_decision.time_window = adjusted_time_window
            routing_decision.routing_context.extra_info["time_window_adjusted"] = True

        return routing_decision
