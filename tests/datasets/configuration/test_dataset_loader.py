from snuba.datasets.factory import get_config_built_datasets
from snuba.datasets.generic_metrics import GenericMetricsDataset


def test_build_entity_from_config_matches_python_definition() -> None:
    py_dataset = GenericMetricsDataset()
    config_dataset = get_config_built_datasets()["generic_metrics"]

    for py_entity, config_entity in zip(
        config_dataset.get_all_entities(), py_dataset.get_all_entities()
    ):
        assert py_entity.__class__ == config_entity.__class__

    assert config_dataset.is_experimental() == py_dataset.is_experimental()
