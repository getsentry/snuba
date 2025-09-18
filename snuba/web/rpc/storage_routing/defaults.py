from snuba.downsampled_storage_tiers import Tier
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
    RoutingDecision,
)


def get_default_routing_decision(
    routing_context: RoutingContext,
) -> RoutingDecision:
    from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
        OutcomesBasedRoutingStrategy,
    )

    return RoutingDecision(
        routing_context=routing_context,
        strategy=OutcomesBasedRoutingStrategy(),
        tier=Tier.TIER_1,
        can_run=True,
    )
