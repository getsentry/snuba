from dataclasses import dataclass, field
from typing import Any, Generic, Optional


from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc import Tin
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import BaseRoutingStrategy

@dataclass
class RoutingContext(Generic[Tin]):
    in_msg: Tin
    timer: Timer
    # build_query: Callable[[TimeSeriesRequest | TraceItemTableRequest], Query]
    # query_settings: HTTPQuerySettings
    query_result: Optional[QueryResult] = field(default=None)
    extra_info: dict[str, Any] = field(default_factory=dict)

    # def to_log_dict(self) -> dict[str, Any]:
    #     query_result: dict[str, Any] = {}
    #     if self.query_result:
    #         query_result["meta"] = self.query_result.result.get("meta", {})
    #         query_result["profile"] = self.query_result.result.get("profile", {})
    #         query_result["stats"] = self.query_result.extra.get("stats")
    #         query_result["sql"] = self.query_result.extra.get("sql")

    #     return {
    #         "source_request_id": self.in_msg.meta.request_id,
    #         "extra_info": self.extra_info,
    #         "clickhouse_settings": self.query_settings.get_clickhouse_settings(),
    #         "result_info": query_result,
    #         "routed_tier": self.query_settings.get_sampling_tier().name,
    #     }

@dataclass
class RoutingDecision(Generic[Tin]):
    routing_context: RoutingContext[Tin]
    strategy: BaseRoutingStrategy | None = None
    tier: Tier = Tier.TIER_1
    clickhouse_settings: dict[str, str] = {}
    can_run: bool | None = None

    def to_log_dict(self) -> dict[str, Any]:
        assert self.routing_context is not None
        query_result: dict[str, Any] = {}
        if self.routing_context.query_result:
            query_result["meta"] = self.routing_context.query_result.result.get(
                "meta", {}
            )
            query_result["profile"] = self.routing_context.query_result.result.get(
                "profile", {}
            )
            query_result["stats"] = self.routing_context.query_result.extra.get("stats")
            query_result["sql"] = self.routing_context.query_result.extra.get("sql")

        return {
            "source_request_id": self.routing_context.in_msg.meta.request_id,  # type: ignore
            "extra_info": self.routing_context.extra_info,
            "clickhouse_settings": self.clickhouse_settings,
            "result_info": query_result,
            "routed_tier": self.tier.name,
        }
