from google.protobuf.message import Message as ProtobufMessage

from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
    RoutingDecision,
)

_DEFAULT_ROUTING_CONTEXT = RoutingContext(
    in_msg=ProtobufMessage(),
    timer=Timer("endpoint_timing"),
)


def get_default_routing_decision(
    routing_context: RoutingContext | None,
) -> RoutingDecision:
    from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
        OutcomesBasedRoutingStrategy,
    )

    return RoutingDecision(
        routing_context=routing_context or _DEFAULT_ROUTING_CONTEXT,
        strategy=OutcomesBasedRoutingStrategy(),
        tier=Tier.TIER_1,
        can_run=True,
    )
