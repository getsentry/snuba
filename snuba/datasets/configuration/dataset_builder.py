from __future__ import annotations

import fastjsonschema
import sentry_sdk

from snuba.datasets.configuration.json_schema import V1_DATASET_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.pluggable_dataset import PluggableDataset

with sentry_sdk.start_span(op="compile", description="Dataset Validators"):
    DATASET_VALIDATORS = {"dataset": fastjsonschema.compile(V1_DATASET_SCHEMA)}


def build_dataset_from_config(config_file_path: str) -> PluggableDataset:
    config = load_configuration_data(config_file_path, DATASET_VALIDATORS)
    return PluggableDataset(
        name=config["name"],
        all_entities=[EntityKey(key) for key in config["entities"]],
        is_experimental=bool(config["is_experimental"]),
    )
