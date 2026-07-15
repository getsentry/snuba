from collections.abc import Mapping
from typing import Any

from snuba import state
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.manual_jobs import Job, JobLogger
from snuba.query.allocation_policies import AllocationPolicy

CBRS_POLICY_CLASS_NAME = "BytesScannedRejectingPolicy"


class LogRuntimeConfigs(Job):
    """Logs all runtime configs (every allocation policy and CBRS values
    included) to snapshot current values ahead of the transition to
    sentry-options.
    """

    def _log_runtime_configs(self, logger: JobLogger) -> None:
        logger.info("========== runtime configs (snuba.state) ==========")
        configs = state.get_all_configs()
        if not configs:
            logger.info("no runtime configs are set")
        else:
            descriptions = state.get_all_config_descriptions()
            for key in sorted(configs.keys()):
                description = descriptions.get(key)
                suffix = f"  # {description}" if description else ""
                logger.info(f"runtime_config {key} = {configs[key]!r}{suffix}")
        logger.info(f"total runtime configs: {len(configs)}")

    def _log_policy(
        self, logger: JobLogger, storage_key: str, policy: AllocationPolicy
    ) -> Mapping[str, Any] | None:
        policy_name = policy.__class__.__name__
        try:
            configs = policy.get_current_configs()
        except Exception as e:
            logger.error(f"[{storage_key}] failed to read configs for policy {policy_name}: {e}")
            return None

        logger.info(
            f"[{storage_key}] allocation_policy {policy_name} "
            f"(resource={policy.resource_identifier.value}): {len(configs)} config(s)"
        )
        for config in configs:
            name = config.get("name")
            value = config.get("value")
            config_params = config.get("params") or {}
            param_suffix = f" params={config_params}" if config_params else ""
            logger.info(f"    {name} = {value!r}{param_suffix}")

        if policy_name == CBRS_POLICY_CLASS_NAME:
            return {
                "storage_key": storage_key,
                "resource": policy.resource_identifier.value,
                "configs": configs,
            }
        return None

    def _log_allocation_policies(self, logger: JobLogger) -> None:
        logger.info("========== allocation policy configs ==========")
        cbrs_records: list[Mapping[str, Any]] = []

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
                record = self._log_policy(logger, storage_key.value, policy)
                if record is not None:
                    cbrs_records.append(record)

        self._log_cbrs_summary(logger, cbrs_records)

    def _log_cbrs_summary(self, logger: JobLogger, cbrs_records: list[Mapping[str, Any]]) -> None:
        logger.info(f"========== CBRS ({CBRS_POLICY_CLASS_NAME}) summary ==========")
        if not cbrs_records:
            logger.info(f"no {CBRS_POLICY_CLASS_NAME} policies found")
            return
        for record in cbrs_records:
            logger.info(f"[{record['storage_key']}] resource={record['resource']}")
            for config in record["configs"]:
                name = config.get("name")
                value = config.get("value")
                config_params = config.get("params") or {}
                param_suffix = f" params={config_params}" if config_params else ""
                logger.info(f"    {name} = {value!r}{param_suffix}")

    def execute(self, logger: JobLogger) -> None:
        logger.info("logging all runtime configs")
        self._log_runtime_configs(logger)
        self._log_allocation_policies(logger)
        logger.info("done logging all runtime configs")
