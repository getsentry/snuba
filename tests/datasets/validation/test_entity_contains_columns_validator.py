import pytest

from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityContainsColumnsValidator,
)

entity_contains_columns_tests = [
    pytest.param(
        "tests/datasets/configuration/entity_with_fixed_string.yaml",
        id="Validate Entity Columns",
    ),
    pytest.param(
        "tests/datasets/configuration/entity_with_nested_field.yaml",
        id="Validate nested columns",
    ),
]


@pytest.mark.parametrize("config_path", entity_contains_columns_tests)
def test_outcomes_columns_validation(config_path: str) -> None:
    entity = build_entity_from_config(config_path)

    query_entity = QueryEntity(entity.entity_key, entity.get_data_model())

    bad_query = LogicalQuery(
        query_entity,
        selected_columns=[
            SelectedExpression("asdf", Column("_snuba_asdf", None, "asdf")),
            *[
                SelectedExpression(
                    column.name, Column(f"_snuba_{column.name}", None, column.name)
                )
                for column in entity.get_data_model().columns
            ],
        ],
    )

    good_query = LogicalQuery(
        query_entity,
        selected_columns=[
            SelectedExpression(
                column.name, Column(f"_snuba_{column.name}", None, column.name)
            )
            for column in entity.get_data_model().columns
        ],
    )

    validator = EntityContainsColumnsValidator(
        entity.get_data_model(), validation_mode=ColumnValidationMode.ERROR
    )

    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)

    validator.validate(good_query)


@pytest.mark.parametrize("config_path", entity_contains_columns_tests)
def test_in_where_clause_and_function(config_path):
    entity = build_entity_from_config(config_path)

    query_entity = QueryEntity(entity.entity_key, entity.get_data_model())

    bad_query = LogicalQuery(
        query_entity,
        selected_columns=[
            *[
                SelectedExpression(
                    column.name, Column(f"_snuba_{column.name}", None, column.name)
                )
                for column in entity.get_data_model().columns
            ],
        ],
        condition=FunctionCall(None, "f1", (Column(None, None, "bad_column"),)),
    )
    validator = EntityContainsColumnsValidator(
        entity.get_data_model(), validation_mode=ColumnValidationMode.ERROR
    )

    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)
