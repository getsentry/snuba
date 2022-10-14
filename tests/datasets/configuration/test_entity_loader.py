import pytest
from jsonschema.exceptions import ValidationError

from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_entity import PluggableEntity
from tests.datasets.configuration.utils import ConfigurationTest


class TestEntityConfiguration(ConfigurationTest):
    def test_build_entity_from_config_matches_python_definition(self) -> None:
        config_sets_entity = build_entity_from_config(
            "snuba/datasets/configuration/generic_metrics/entities/sets.yaml"
        )
        py_sets_entity = get_entity(EntityKey.GENERIC_METRICS_SETS)

        assert isinstance(config_sets_entity, PluggableEntity)
        assert config_sets_entity.entity_key == EntityKey.GENERIC_METRICS_SETS

        for (config_qp, py_qp) in zip(
            config_sets_entity.get_query_processors(),
            py_sets_entity.get_query_processors(),
        ):
            assert (
                config_qp.__class__ == py_qp.__class__
            ), "query processor mismatch between configuration-loaded sets and python-defined"

        for (config_v, py_v) in zip(
            config_sets_entity.get_validators(), py_sets_entity.get_validators()
        ):
            assert (
                config_v.__class__ == py_v.__class__
            ), "validator mismatch between configuration-loaded sets and python-defined"

        assert (
            config_sets_entity.get_all_storages() == py_sets_entity.get_all_storages()
        )
        assert (
            config_sets_entity.get_writable_storage()
            == py_sets_entity.get_writable_storage()
        )
        assert (
            config_sets_entity.required_time_column
            == py_sets_entity.required_time_column
        )

        assert config_sets_entity.get_data_model() == py_sets_entity.get_data_model()

    def test_bad_configuration_broken_query_processor(self) -> None:
        with pytest.raises(ValidationError):
            build_entity_from_config(
                "tests/datasets/configuration/broken_entity_bad_query_processor.yaml"
            )

    def test_bad_configuration_broken_validator(self) -> None:
        with pytest.raises(ValidationError):
            build_entity_from_config(
                "tests/datasets/configuration/broken_entity_positional_validator_args.yaml"
            )
