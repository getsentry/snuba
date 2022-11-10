from unittest import mock

from snuba.core.initialize import initialize_snuba


class TestInitialization:
    def test_init(
        self,
    ):
        # first make sure all the factories are not initialized
        # this is accessing private module variables but we don't have a
        # better way of knowing things are initialized (2022-10-27)
        from snuba.datasets.entities.factory import _ENT_FACTORY
        from snuba.datasets.factory import _DS_FACTORY
        from snuba.datasets.storages.factory import _STORAGE_FACTORY

        for factory in (_DS_FACTORY, _ENT_FACTORY, _STORAGE_FACTORY):
            assert factory is None

        initialize_snuba()
        from snuba.datasets.entities.factory import get_all_entity_names
        from snuba.datasets.factory import get_enabled_dataset_names
        from snuba.datasets.storages.factory import get_all_storage_keys

        # now that we have called the initialize function. We should not have to
        # load the entities anymore
        with mock.patch("snuba.datasets.configuration.loader.safe_load") as load_func:
            for factory_func in (
                get_enabled_dataset_names,
                get_all_entity_names,
                get_all_storage_keys,
            ):
                factory_func()
            load_func.assert_not_called()
