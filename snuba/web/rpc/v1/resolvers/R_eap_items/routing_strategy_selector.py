import json
from dataclasses import dataclass

import sentry_sdk

from snuba.state import get_config
from snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.linear_bytes_scanned_storage_routing import (
    LinearBytesScannedRoutingStrategy,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing import BaseRoutingStrategy

_FLOATING_POINT_TOLERANCE = 1e-6
_STORAGE_ROUTING_CONFIG_KEY = "storage_routing_config"


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

            version = config_dict["version"]
            config_strategies = {k: v for k, v in config_dict.items() if k != "version"}

            routing_strategy_and_percentage_routed = {}
            total_percentage = 0.0
            for strategy_name, percentage in config_strategies.items():
                if percentage < 0 or percentage > 1:
                    raise ValueError(
                        f"Percentage for {strategy_name} needs to be a float between 0 and 1"
                    )

                try:
                    BaseRoutingStrategy.get_from_name(strategy_name)()  # type: ignore
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
        LinearBytesScannedRoutingStrategy: 1.0,
    },
)


class RoutingStrategySelector:
    def get_storage_routing_strategy_config(self) -> StorageRoutingConfig:
        config = str(get_config(_STORAGE_ROUTING_CONFIG_KEY, "{}"))
        return StorageRoutingConfig.from_json(config)
