from typing import Any

import pytest
from jsonschema.exceptions import ValidationError

from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToMapping,
)
from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.query.expressions import Column, FunctionCall


def test_build_entity_from_config_matches_python_definition() -> None:
    config_sets_entity = build_entity_from_config(
        "snuba/datasets/configuration/generic_metrics/entities/sets.yaml"
    )
    py_sets_entity = get_entity(EntityKey.GENERIC_METRICS_SETS)

    assert isinstance(config_sets_entity, PluggableEntity)
    assert config_sets_entity.entity_key == EntityKey.GENERIC_METRICS_SETS

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

    assert config_sets_entity.get_data_model() == py_sets_entity.get_data_model()


def test_bad_configuration_broken_query_processor() -> None:
    with pytest.raises(ValidationError):
        build_entity_from_config(
            "tests/datasets/configuration/broken_entity_bad_query_processor.yaml"
        )


def test_bad_configuration_broken_validator() -> None:
    with pytest.raises(ValidationError):
        build_entity_from_config(
            "tests/datasets/configuration/broken_entity_positional_validator_args.yaml"
        )


def get_object_in_list_by_class(object_list: Any, object_class: Any) -> Any:
    for obj in object_list:
        if isinstance(obj, object_class):
            return obj
    return None


def test_entity_loader_for_enitity_with_column_mappers() -> None:
    pluggable_entity = build_entity_from_config(
        "tests/datasets/configuration/entity_with_ip_column_mappers.yaml"
    )
    column_mappers = pluggable_entity.translation_mappers.columns

    # Check that ColumnToFunction mapper was successfully loaded from config
    column_to_function = get_object_in_list_by_class(column_mappers, ColumnToFunction)
    assert isinstance(column_to_function, ColumnToFunction)

    # Check that nested expressions were loaded correctly in ColumnToFunction
    assert len(column_to_function.to_function_params) == 2
    function_call = next(
        (
            fc
            for fc in column_to_function.to_function_params
            if isinstance(fc, FunctionCall) and fc.function_name == "IPv4NumToString"
        ),
        None,
    )
    assert function_call is not None
    assert len(function_call.parameters) == 1
    assert any(isinstance(param, Column) for param in function_call.parameters)

    # Check that other column mappers (which do not contain expressions) were loaded correctly
    column_to_mapping = get_object_in_list_by_class(column_mappers, ColumnToMapping)
    assert column_to_mapping is not None
    assert column_to_mapping.from_col_name == "geo_country_code"
    column_to_column = get_object_in_list_by_class(column_mappers, ColumnToColumn)
    assert column_to_column is not None
    assert column_to_column.from_col_name == "email"
