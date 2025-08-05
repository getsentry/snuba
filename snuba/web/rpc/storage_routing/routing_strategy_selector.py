import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Tuple

import sentry_sdk
from google.protobuf.message import Message as ProtobufMessage

from snuba import settings
from snuba.state import get_config
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
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
        "OutcomesBasedRoutingStrategy": 1.0,
    },
)


class RoutingStrategySelector:
    def get_storage_routing_config(
        self, in_msg: ProtobufMessage
    ) -> StorageRoutingConfig:
        in_msg_meta = extract_message_meta(in_msg)
        organization_id = str(in_msg_meta.organization_id)
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
        try:
            in_msg_meta = extract_message_meta(routing_context.in_msg)
            combined_org_and_project_ids = f"{in_msg_meta.organization_id}:{'.'.join(str(pid) for pid in sorted(in_msg_meta.project_ids))}"
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
        except Exception as e:
            if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
                raise e
            sentry_sdk.capture_message(f"Error selecting routing strategy: {e}")
            return OutcomesBasedRoutingStrategy()

        # this should never happen because the percentages were validated in StorageRoutingConfig.from_json
        return OutcomesBasedRoutingStrategy()
