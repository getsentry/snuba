from typing import Any, Type

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
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.factory import reset_dataset_factory
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.query.expressions import Column, FunctionCall, Literal
from tests.datasets.configuration.utils import ConfigurationTest


def get_object_in_list_by_class(object_list: Any, object_class: Any) -> Any:
    for obj in object_list:
        if isinstance(obj, object_class):
            return obj
    return None


class TestEntityConfigurationComparison(ConfigurationTest):
    """
    This test compare the YAML config files to the Python ones.
    This test suite is only useful as we translate entities to YAML.
    Once all the entities are YAML and the Python ones are removed this
    test suite can also be removed.
    """

    def setup_class(self) -> None:
        reset_dataset_factory()

        from snuba.datasets.cdc.groupassignee_entity import GroupAssigneeEntity
        from snuba.datasets.cdc.groupedmessage_entity import GroupedMessageEntity
        from snuba.datasets.entities.generic_metrics import GenericMetricsSetsEntity
        from snuba.datasets.entities.outcomes import OutcomesEntity
        from snuba.datasets.entities.outcomes_raw import OutcomesRawEntity
        from snuba.datasets.entities.transactions import TransactionsEntity

        self.test_data = [
            (
                "snuba/datasets/configuration/generic_metrics/entities/sets.yaml",
                GenericMetricsSetsEntity,
                EntityKey.GENERIC_METRICS_SETS,
            ),
            (
                "snuba/datasets/configuration/transactions/entities/transactions.yaml",
                TransactionsEntity,
                EntityKey.TRANSACTIONS,
            ),
            (
                "snuba/datasets/configuration/groupassignee/entities/groupassignee.yaml",
                GroupAssigneeEntity,
                EntityKey.GROUPASSIGNEE,
            ),
            (
                "snuba/datasets/configuration/groupedmessage/entities/groupedmessage.yaml",
                GroupedMessageEntity,
                EntityKey.GROUPEDMESSAGE,
            ),
            (
                "snuba/datasets/configuration/outcomes/entities/outcomes.yaml",
                OutcomesEntity,
                EntityKey.OUTCOMES,
            ),
            (
                "snuba/datasets/configuration/outcomes/entities/outcomes_raw.yaml",
                OutcomesRawEntity,
                EntityKey.OUTCOMES_RAW,
            ),
        ]

    def _config_matches_python_definition(
        self, config_path: str, entity: Type[Entity], entity_key: EntityKey
    ) -> None:
        config_entity = build_entity_from_config(config_path)
        py_entity = entity()  # type: ignore

        assert isinstance(config_entity, PluggableEntity), entity_key.value
        assert config_entity.entity_key == entity_key, entity_key.value

        assert len(config_entity.get_query_processors()) == len(
            py_entity.get_query_processors()
        ), entity_key.value
        for (config_qp, py_qp) in zip(
            config_entity.get_query_processors(), py_entity.get_query_processors()
        ):
            assert (
                config_qp.__class__ == py_qp.__class__
            ), f"{entity_key.value}: query processor mismatch between configuration-loaded sets and python-defined"

        assert len(config_entity.get_validators()) == len(
            py_entity.get_validators()
        ), entity_key.value
        for (config_v, py_v) in zip(
            config_entity.get_validators(), py_entity.get_validators()
        ):
            assert (
                config_v.__class__ == py_v.__class__
            ), f"{entity_key.value}: validator mismatch between configuration-loaded sets and python-defined"
            assert config_v.__dict__ == py_v.__dict__, entity_key.value

        assert (
            config_entity.get_all_storages() == py_entity.get_all_storages()
        ), entity_key.value
        assert (
            config_entity.required_time_column == py_entity.required_time_column
        ), entity_key.value

        assert (
            config_entity.get_data_model() == py_entity.get_data_model()
        ), entity_key.value

    def test_config_matches_python_definition(self) -> None:
        for test in self.test_data:
            self._config_matches_python_definition(*test)  # type: ignore


class TestEntityConfiguration(ConfigurationTest):
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
