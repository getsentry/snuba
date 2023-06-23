from typing import Any

import pytest
from fastjsonschema.exceptions import JsonSchemaValueException

from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToIPAddress,
    ColumnToMapping,
    ColumnToNullIf,
)
from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.query.expressions import Column, FunctionCall, Literal
from tests.datasets.configuration.utils import ConfigurationTest


def get_object_in_list_by_class(object_list: Any, object_class: Any) -> Any:
    for obj in object_list:
        if isinstance(obj, object_class):
            return obj
    return None


class TestEntityConfiguration(ConfigurationTest):
    def test_entity_loader_fixed_string(self) -> None:
        entity = build_entity_from_config(
            "tests/datasets/configuration/entity_with_fixed_string.yaml"
        )
        columns = list(entity.get_data_model())
        assert len(columns) == 3
        assert columns[0].type.length == 420  # type: ignore
        assert columns[2].type.length == 69  # type: ignore

    def test_bad_configuration_broken_query_processor(self) -> None:
        with pytest.raises(JsonSchemaValueException):
            build_entity_from_config(
                "tests/datasets/configuration/broken_entity_bad_query_processor.yaml"
            )

    def test_bad_configuration_broken_validator(self) -> None:
        with pytest.raises(JsonSchemaValueException):
            build_entity_from_config(
                "tests/datasets/configuration/broken_entity_positional_validator_args.yaml"
            )

    def test_entity_loader_for_entity_with_column_mappers(self) -> None:
        pluggable_entity = build_entity_from_config(
            "tests/datasets/configuration/entity_with_column_mappers.yaml"
        )
        column_mappers = pluggable_entity.get_all_storage_connections()[
            0
        ].translation_mappers.columns

        # Check that ColumnToIpAdress mapper was successfully loaded from config
        column_to_ip_address = get_object_in_list_by_class(
            column_mappers, ColumnToIPAddress
        )
        assert isinstance(column_to_ip_address, ColumnToFunction)

        # Check that nested expressions were loaded correctly in ColumnToIPAddress
        assert len(column_to_ip_address.to_function_params) == 2
        function_call = next(
            (
                fc
                for fc in column_to_ip_address.to_function_params
                if isinstance(fc, FunctionCall)
                and fc.function_name == "IPv4NumToString"
            ),
            None,
        )
        assert function_call is not None
        assert len(function_call.parameters) == 1
        assert any(isinstance(param, Column) for param in function_call.parameters)

        # Check that ColumnToNullIf mapper was successfully loaded from config
        column_to_user_null_if = get_object_in_list_by_class(
            column_mappers, ColumnToNullIf
        )
        assert isinstance(column_to_user_null_if, ColumnToFunction)

        # Check that expressions were loaded correctly in ColumnToNullIf
        assert len(column_to_user_null_if.to_function_params) == 2
        assert any(
            isinstance(param, Column)
            for param in column_to_user_null_if.to_function_params
        )
        assert any(
            isinstance(param, Literal)
            for param in column_to_user_null_if.to_function_params
        )

        # Check that other column mappers (which do not contain expressions) were loaded correctly
        column_to_mapping = get_object_in_list_by_class(column_mappers, ColumnToMapping)
        assert column_to_mapping is not None
        assert column_to_mapping.from_col_name == "geo_country_code"
        column_to_column = get_object_in_list_by_class(column_mappers, ColumnToColumn)
        assert column_to_column is not None
        assert column_to_column.from_col_name == "email"

    def test_entity_loader_no_custom_validators(self) -> None:
        pluggable_entity = build_entity_from_config(
            "tests/datasets/configuration/entity_no_custom_validators.yaml"
        )
        entity_validators = set(pluggable_entity.get_validators())
        assert len(entity_validators) == len(pluggable_entity._get_builtin_validators())

    @pytest.mark.skip(reason="Dataset no longer exists")
    def test_entity_loader_join_relationships(self) -> None:
        pluggable_entity = build_entity_from_config(
            "tests/datasets/configuration/entity_join_relationships.yaml"
        )
        relationships = pluggable_entity.get_all_join_relationships()
        assert len(relationships) == 1
        rel = pluggable_entity.get_join_relationship("owns")
        assert rel is not None
        assert rel.rhs_entity.value == "events"
        assert rel.join_type.value == "LEFT"
        assert len(rel.columns) == 2
        assert rel.columns[0][0] == "project_id"
        assert rel.columns[0][1] == "project_id"
        assert rel.columns[1][0] == "group_id"
        assert rel.columns[1][1] == "group_id"
        assert len(rel.equivalences) == 1
        assert rel.equivalences[0][0] == "offset"
        assert rel.equivalences[0][1] == "offset"
