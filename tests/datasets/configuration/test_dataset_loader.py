from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_config_built_datasets
from snuba.datasets.generic_metrics import GenericMetricsDataset
from snuba.datasets.transactions import TransactionsDataset
from tests.datasets.configuration.utils import ConfigurationTest


class TestDatasetConfiguration(ConfigurationTest):
    def test_build_entity_from_config_matches_python_definition(self) -> None:
        test_data = [
            (GenericMetricsDataset(), get_config_built_datasets()["generic_metrics"]),
            (TransactionsDataset(), get_config_built_datasets()["transactions"]),
        ]
        for test in test_data:
            self._dataset_config_matches_python_definition(*test)

    def _dataset_config_matches_python_definition(
        self, py_dataset: Dataset, config_dataset: Dataset
    ) -> None:
        for py_entity, config_entity in zip(
            config_dataset.get_all_entities(), py_dataset.get_all_entities()
        ):
            assert py_entity.__class__ == config_entity.__class__

        assert config_dataset.is_experimental() == py_dataset.is_experimental()
