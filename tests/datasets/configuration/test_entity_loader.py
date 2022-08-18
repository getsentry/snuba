from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_entity import PluggableEntity


def test_build_entity_from_config_matches_python_definition() -> None:
    config_sets_entity = build_entity_from_config(
        "snuba/datasets/configuration/generic_metrics/entities/sets.yaml"
    )
    py_sets_entity = get_entity(EntityKey.GENERIC_METRICS_SETS)

    assert isinstance(config_sets_entity, PluggableEntity)
    assert config_sets_entity.name == "generic_metrics_sets"

    for (config_qp, py_qp) in zip(
        config_sets_entity.get_query_processors(), py_sets_entity.get_query_processors()
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

    assert config_sets_entity.get_all_storages() == py_sets_entity.get_all_storages()
    assert (
        config_sets_entity.get_writable_storage()
        == py_sets_entity.get_writable_storage()
    )
    assert (
        config_sets_entity.required_time_column == py_sets_entity.required_time_column
    )

    # TODO add tests for TranslationMappers equality
    # TODO add tests for schema equality
