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

    def __init__(self) -> None:
        self._outcomes_based_routing_strategy = OutcomesBasedRoutingStrategy()
        self._load_based_routing_strategy = LoadBasedRoutingStrategy()

    def _additional_config_definitions(self) -> list[Configuration]:
        return (
            self._outcomes_based_routing_strategy.additional_config_definitions()
            + self._load_based_routing_strategy.additional_config_definitions()
        )

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        self._load_based_routing_strategy._update_routing_decision(routing_decision)
        self._outcomes_based_routing_strategy._update_routing_decision(routing_decision)
