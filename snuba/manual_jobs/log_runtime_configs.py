import json
from typing import Any

from snuba import state
from snuba.configs.configuration import CONFIGURABLE_COMPONENT_OVERRIDES_KEY
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.manual_jobs import Job, JobLogger

CBRS_POLICY_CLASS_NAME = "BytesScannedRejectingPolicy"

PAYLOAD_START_MARKER = "===== BEGIN SENTRY-OPTIONS PAYLOAD ====="
PAYLOAD_END_MARKER = "===== END SENTRY-OPTIONS PAYLOAD ====="


class LogRuntimeConfigs(Job):
    """Dumps all runtime configs (every allocation policy and CBRS values
    included) as a single JSON payload that can be pasted into an LLM to
    generate the equivalent sentry-options config.

    The ``configurable_component_overrides`` section is keyed exactly like the
    sentry-options override dict of the same name, so an LLM has everything it
    needs to produce the new options.
    """

    def _collect_runtime_configs(self) -> dict[str, Any]:
        return dict(sorted(state.get_all_configs().items()))

    def _collect_component_overrides(self, logger: JobLogger) -> dict[str, Any]:
        overrides: dict[str, Any] = {}
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        ):
            try:
                storage = get_storage(storage_key)
                policies = (
                    storage.get_allocation_policies() + storage.get_delete_allocation_policies()
                )
            except Exception as e:
                logger.error(f"[{storage_key.value}] failed to read allocation policies: {e}")
                continue

            for policy in policies:
                policy_name = policy.__class__.__name__
                try:
                    configs = policy.get_current_configs()
                except Exception as e:
                    logger.error(
                        f"[{storage_key.value}] failed to read configs for "
                        f"policy {policy_name}: {e}"
                    )
                    continue
                for config in configs:
                    key = policy._build_runtime_config_key(
                        config["name"], config.get("params") or {}
                    )
                    overrides[key] = config.get("value")
        return dict(sorted(overrides.items()))

    def execute(self, logger: JobLogger) -> None:
        component_overrides = self._collect_component_overrides(logger)
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
