from snuba.downsampled_storage_tiers import Tier
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
    RoutingDecision,
)


class HighestAccuracyRoutingStrategy(BaseRoutingStrategy):
    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        return RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_1,
            clickhouse_settings={},
            can_run=True,
        )
