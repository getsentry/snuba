import hashlib
import json
from dataclasses import dataclass

import sentry_sdk

from snuba.state import get_config
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.linear_bytes_scanned_storage_routing import (
    LinearBytesScannedRoutingStrategy,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
)

_FLOATING_POINT_TOLERANCE = 1e-6
_DEFUALT_STORAGE_ROUTING_CONFIG_KEY = "default_storage_routing_config"
_NUM_BUCKETS = 100


@dataclass
class StorageRoutingConfig:
    version: int
    routing_strategy_and_percentage_routed: dict[str, float]

    @classmethod
    def from_json(cls, config_json: str) -> "StorageRoutingConfig":
        try:
            config_dict = json.loads(config_json)
            if "version" not in config_dict or not isinstance(
                config_dict["version"], int
            ):
                raise ValueError("please specify version as an integer")
            if "config" not in config_dict or not isinstance(
                config_dict["config"], dict
            ):
                raise ValueError("please specify config as a dict")

            version = config_dict["version"]
            config_strategies = config_dict["config"]

            routing_strategy_and_percentage_routed = {}
            total_percentage = 0.0
            for strategy_name, percentage in config_strategies.items():
                if percentage < 0 or percentage > 1:
                    raise ValueError(
                        f"Percentage for {strategy_name} needs to be a float between 0 and 1"
                    )

                try:
                    BaseRoutingStrategy.get_from_name(strategy_name)()
                except Exception:
                    raise ValueError(
                        f"{strategy_name} does not inherit from BaseRoutingStrategy"
                    )
                routing_strategy_and_percentage_routed[strategy_name] = percentage

                total_percentage += percentage

            if abs(total_percentage - 1.0) > _FLOATING_POINT_TOLERANCE:
                raise ValueError("Total percentage must add up to 1.0")

            return cls(
                version=version,
                routing_strategy_and_percentage_routed=routing_strategy_and_percentage_routed,
            )
        except Exception as e:
            sentry_sdk.capture_message(f"Error parsing storage routing config: {e}")
            return _DEFAULT_STORAGE_ROUTING_CONFIG


_DEFAULT_STORAGE_ROUTING_CONFIG = StorageRoutingConfig(
    version=1,
    routing_strategy_and_percentage_routed={
        "LinearBytesScannedRoutingStrategy": 1.0,
    },
)


class RoutingStrategySelector:
    def get_storage_routing_config(self) -> StorageRoutingConfig:
        config = str(get_config(_DEFUALT_STORAGE_ROUTING_CONFIG_KEY, "{}"))
        return StorageRoutingConfig.from_json(config)

    def select_routing_strategy(
        self, routing_context: RoutingContext
    ) -> BaseRoutingStrategy:
        config = self.get_storage_routing_config()

        combined_org_and_project_ids = f"{routing_context.in_msg.meta.organization_id}:{'.'.join(str(pid) for pid in sorted(routing_context.in_msg.meta.project_ids))}"
        bucket = (
            int(hashlib.md5(combined_org_and_project_ids.encode()).hexdigest(), 16)
            % _NUM_BUCKETS
        )

        cumulative_buckets = 0.0
        for (
            strategy_name,
            percentage,
        ) in config.routing_strategy_and_percentage_routed.items():
            cumulative_buckets += percentage * _NUM_BUCKETS
            if bucket < cumulative_buckets:
                return BaseRoutingStrategy.get_from_name(strategy_name)()

        # this should never happen because the percentages were validated in StorageRoutingConfig.from_json
        return LinearBytesScannedRoutingStrategy()
