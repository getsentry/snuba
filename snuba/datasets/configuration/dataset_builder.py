from __future__ import annotations

from snuba.datasets.configuration.json_schema import V1_DATASET_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entities import EntityKey
from snuba.datasets.pluggable_dataset import PluggableDataset

DATASET_VALIDATION_SCHEMAS = {"dataset": V1_DATASET_SCHEMA}


def build_dataset(config_file_path: str) -> PluggableDataset:
    config = load_configuration_data(config_file_path, DATASET_VALIDATION_SCHEMAS)
    return PluggableDataset(
        name=config["name"],
        is_experimental=bool(config["is_experimental"]),
        default_entity=EntityKey(config["entities"]["default"]),
        all_entities=[EntityKey(key) for key in config["entities"]["all"]],
    )
