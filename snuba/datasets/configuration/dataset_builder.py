from __future__ import annotations

import sentry_sdk

from snuba.datasets.configuration.json_schema import V1_DATASET_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.pluggable_dataset import PluggableDataset

DATASET_VALIDATION_SCHEMAS = {"dataset": V1_DATASET_SCHEMA}


def build_dataset_from_config(config_file_path: str) -> PluggableDataset:
    config = load_configuration_data(config_file_path, DATASET_VALIDATION_SCHEMAS)
    with sentry_sdk.start_span(op="build", description="Dataset") as span:
        span.set_tag("dataset", config["name"])
        return PluggableDataset(
            name=config["name"],
            all_entities=[EntityKey(key) for key in config["entities"]["all"]],
            is_experimental=bool(config["is_experimental"]),
        )
