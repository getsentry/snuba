from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_config_built_datasets
from snuba.datasets.metrics import MetricsDataset
from snuba.datasets.replays import ReplaysDataset
from snuba.datasets.sessions import SessionsDataset
from snuba.datasets.transactions import TransactionsDataset
from tests.datasets.configuration.utils import ConfigurationTest


class TestDatasetConfiguration(ConfigurationTest):
    def test_build_entity_from_config_matches_python_definition(self) -> None:
        config_built_datasets = get_config_built_datasets()
        test_data = [
            (TransactionsDataset(), config_built_datasets["transactions"]),
            (MetricsDataset(), config_built_datasets["metrics"]),
            (SessionsDataset(), config_built_datasets["sessions"]),
            (ReplaysDataset(), config_built_datasets["replays"]),
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
