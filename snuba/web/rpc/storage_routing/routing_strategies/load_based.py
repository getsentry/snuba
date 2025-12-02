import sentry_sdk

from snuba.configs.configuration import Configuration
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingDecision,
)


class LoadBasedRoutingStrategy(BaseRoutingStrategy):
    """
    If cluster load is under a threshold, ignore recommendations and allow the query to pass through with the tier decided based on outcomes-based routing.
    """

    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="pass_through_load_percentage",
                description="If cluster load is below this percentage, allow the query to run regardless of allocation policies",
                value_type=int,
                default=20,
            ),
            Configuration(
                name="pass_through_max_threads",
                description="Max threads to use when allowing the query to pass through under low load",
                value_type=int,
                default=10,
            ),
        ]

    def _update_routing_decision(
        self,
        routing_decision: RoutingDecision,
    ) -> None:
        load_info = routing_decision.routing_context.cluster_load_info
        if load_info is None:
            return

        pass_through_threshold = int(self.get_config_value("pass_through_load_percentage"))
        pass_through_max_threads = int(self.get_config_value("pass_through_max_threads"))

        if load_info.cluster_load < pass_through_threshold:
            routing_decision.can_run = True
            routing_decision.is_throttled = False
            routing_decision.clickhouse_settings["max_threads"] = pass_through_max_threads
            routing_decision.routing_context.extra_info["load_based_pass_through"] = {
                "threshold": pass_through_threshold,
                "max_threads": pass_through_max_threads,
            }
            sentry_sdk.update_current_span(  # pyright: ignore[reportUndefinedVariable]
                attributes={
                    "load_based_pass_through": True,
                    "cluster_load": load_info.cluster_load,
                    "pass_through_threshold": pass_through_threshold,
                    "pass_through_max_threads": pass_through_max_threads,
                }
            )
