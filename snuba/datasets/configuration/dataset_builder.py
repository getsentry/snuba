from __future__ import annotations

from snuba.datasets.configuration.json_schema import DATASET_VALIDATORS
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.pluggable_dataset import PluggableDataset


def build_dataset_from_config(config_file_path: str) -> PluggableDataset:
    config = load_configuration_data(config_file_path, DATASET_VALIDATORS)
    return PluggableDataset(
        name=config["name"],
        all_entities=[EntityKey(key) for key in config["entities"]],
    )
