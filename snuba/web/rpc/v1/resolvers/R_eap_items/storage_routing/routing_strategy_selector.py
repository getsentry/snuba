import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Tuple

import sentry_sdk

from snuba.state import get_config
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.linear_bytes_scanned_storage_routing import (
    LinearBytesScannedRoutingStrategy,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutedRequestType,
    RoutingContext,
)

_FLOATING_POINT_TOLERANCE = 1e-6
_DEFAULT_STORAGE_ROUTING_CONFIG_KEY = "default_storage_routing_config"
_STORAGE_ROUTING_CONFIG_OVERRIDE_KEY = "storage_routing_config_override"
_NUM_BUCKETS = 100


@dataclass
class StorageRoutingConfig:
    version: int
    _routing_strategy_and_percentage_routed: dict[str, float]

    def get_routing_strategy_and_percentage_routed(self) -> Iterable[Tuple[str, float]]:
        return sorted(self._routing_strategy_and_percentage_routed.items())

    @classmethod
    def from_json(cls, config_dict: dict[str, Any]) -> "StorageRoutingConfig":
        if config_dict == {}:
            return _DEFAULT_STORAGE_ROUTING_CONFIG
        try:
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
                _routing_strategy_and_percentage_routed=routing_strategy_and_percentage_routed,
            )
        except Exception as e:
            sentry_sdk.capture_message(f"Error parsing storage routing config: {e}")
            return _DEFAULT_STORAGE_ROUTING_CONFIG


_DEFAULT_STORAGE_ROUTING_CONFIG = StorageRoutingConfig(
    version=1,
    _routing_strategy_and_percentage_routed={
        "LinearBytesScannedRoutingStrategy": 1.0,
    },
)


class RoutingStrategySelector:
    def get_storage_routing_config(
        self, in_msg: RoutedRequestType
    ) -> StorageRoutingConfig:
        organization_id = str(in_msg.meta.organization_id)
        try:
            overrides = json.loads(
                str(get_config(_STORAGE_ROUTING_CONFIG_OVERRIDE_KEY, "{}"))
            )
            if organization_id in overrides.keys():
                return StorageRoutingConfig.from_json(overrides[organization_id])

            config = str(get_config(_DEFAULT_STORAGE_ROUTING_CONFIG_KEY, "{}"))
            return StorageRoutingConfig.from_json(json.loads(config))
        except Exception as e:
            sentry_sdk.capture_message(f"Error getting storage routing config: {e}")
            return _DEFAULT_STORAGE_ROUTING_CONFIG

    def select_routing_strategy(
        self, routing_context: RoutingContext
    ) -> BaseRoutingStrategy:
        combined_org_and_project_ids = f"{routing_context.in_msg.meta.organization_id}:{'.'.join(str(pid) for pid in sorted(routing_context.in_msg.meta.project_ids))}"
        bucket = (
            int(hashlib.md5(combined_org_and_project_ids.encode()).hexdigest(), 16)
            % _NUM_BUCKETS
        )
        config = self.get_storage_routing_config(routing_context.in_msg)
        cumulative_buckets = 0.0
        for (
            strategy_name,
            percentage,
        ) in config.get_routing_strategy_and_percentage_routed():
            cumulative_buckets += percentage * _NUM_BUCKETS
            if bucket < cumulative_buckets:
                return BaseRoutingStrategy.get_from_name(strategy_name)()

        # this should never happen because the percentages were validated in StorageRoutingConfig.from_json
        return LinearBytesScannedRoutingStrategy()
