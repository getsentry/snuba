"""
This file contains the initialization sequence for snuba. Anything that needs to be created before a snuba command
is able to run is defined in this file under the `initialize` function


All imports are hidden behind their respective functions to avoid running any code at initialization time unless explicitly
directed
"""

import logging

logger = logging.getLogger("snuba_core_init")


def _load_datasets() -> None:
    from snuba.datasets.factory import reset_dataset_factory

    reset_dataset_factory()


def _load_storages() -> None:
    from snuba.datasets.storages.factory import initialize_storage_factory

    initialize_storage_factory()


def _load_entities() -> None:
    from snuba.datasets.entities.factory import initialize_entity_factory

    initialize_entity_factory()


def initialize_snuba() -> None:
    logger.info("Initializing Snuba...")

    # The order of the functions matters The reference direction is
    #
    #       datasets -> entities -> storages
    #
    # The initialization goes bottom up, starting from the
    # lowest-level concept (storage) to the highest (dataset).
    _load_storages()
    _load_entities()
    _load_datasets()
