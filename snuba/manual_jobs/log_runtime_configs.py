import json
from typing import Any

from snuba import state
from snuba.configs.configuration import CONFIGURABLE_COMPONENT_OVERRIDES_KEY
from snuba.manual_jobs import Job, JobLogger

CBRS_POLICY_CLASS_NAME = "BytesScannedRejectingPolicy"

PAYLOAD_START_MARKER = "===== BEGIN SENTRY-OPTIONS PAYLOAD ====="
PAYLOAD_END_MARKER = "===== END SENTRY-OPTIONS PAYLOAD ====="


class LogRuntimeConfigs(Job):
    """Dumps all runtime configs (every allocation policy and CBRS values
    included) as a single JSON payload that can be pasted into an LLM to
    generate the equivalent sentry-options config.

    This exists to help migrate allocation-policy (and storage-routing-strategy)
    config off the legacy Redis runtime config and onto the
    ``configurable_component_overrides`` sentry-option (see
    getsentry/snuba#8168). The ``configurable_component_overrides`` section
    holds the values currently set in Redis, keyed exactly like that
    sentry-option (``{resource}.{ClassName}.{config}[.{param}:{value},...]``),
    so the output is a ready-made migration payload.
    """

    def _collect_runtime_configs(self) -> dict[str, Any]:
        return dict(sorted(state.get_all_configs().items()))

    def _collect_component_overrides(self) -> dict[str, Any]:
        # ConfigurableComponent configs live in the Redis hashes named by each
        # component's `_get_hash()`: `capman` for every allocation policy
        # (including the EAP CBRS policy attached only to a routing strategy)
        # and `cbrs` for the storage-routing strategies. Reading both gives the
        # full set of overrides that feeds the combined
        # `configurable_component_overrides` sentry-option.
        from snuba.query.allocation_policies import CAPMAN_HASH
        from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
            CBRS_HASH,
        )

        overrides: dict[str, Any] = {}
        for hash_name in (CAPMAN_HASH, CBRS_HASH):
            overrides.update(state.get_all_configs(config_key=hash_name))
        return dict(sorted(overrides.items()))

    def execute(self, logger: JobLogger) -> None:
        component_overrides = self._collect_component_overrides()
        payload = {
            "runtime_configs": self._collect_runtime_configs(),
            CONFIGURABLE_COMPONENT_OVERRIDES_KEY: component_overrides,
            "cbrs": {
                key: value
                for key, value in component_overrides.items()
                if CBRS_POLICY_CLASS_NAME in key
            },
        }
        logger.info(PAYLOAD_START_MARKER)
        logger.info(json.dumps(payload, indent=2, sort_keys=True, default=str))
        logger.info(PAYLOAD_END_MARKER)
