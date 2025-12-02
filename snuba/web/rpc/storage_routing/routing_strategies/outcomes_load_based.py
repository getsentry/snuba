from snuba.configs.configuration import Configuration
from snuba.web.rpc.storage_routing.routing_strategies.load_based import (
    LoadBasedRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingDecision,
)


class OutcomesThenLoadBasedRoutingStrategy(BaseRoutingStrategy):
    """
    Chains outcomes-based routing followed by load-based adjustments.
    """

    def _additional_config_definitions(self) -> list[Configuration]:
        # No additional configs; children read their own configs by class name.
        return []

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        OutcomesBasedRoutingStrategy()._update_routing_decision(routing_decision)
        LoadBasedRoutingStrategy()._update_routing_decision(routing_decision)
