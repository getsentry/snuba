"""
This file contains the initialization sequence for snuba. Anything that needs to be created before a snuba command
is able to run is defined in this file under the `initialize` function


All imports are hidden behind their respective functions to avoid running any code at initialization time unless explicitly
directed
"""
import logging

logger = logging.getLogger("snuba_core_init")


def _load_datasets() -> None:
    from snuba.datasets.factory import get_enabled_dataset_names

    get_enabled_dataset_names()


def _load_storages() -> None:
    from snuba.datasets.storages.factory import get_all_storage_keys

    get_all_storage_keys()


def _load_entities() -> None:
    from snuba.datasets.entities.factory import get_all_entity_names

    get_all_entity_names()


def initialize() -> None:
    logger.info("Initializing snuba")

    # The order of the functions matters. At time of writing (2022-10-27), snuba datasets are not guaranteed to load
    # entities and storages. This is because datasets defined in code reference entities via EntityKey(s) and are
    # thus loaded lazily

    # The initialization goes bottom up, starting from the lowest-level concept (storage) to the highest (dataset)
    _load_storages()
    _load_entities()
    _load_datasets()
