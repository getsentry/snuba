import pytest

from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entity import Entity
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityContainsColumnsValidator,
)

CONFIG_PATH = "tests/datasets/configuration/column_validation_entity.yaml"


def get_validator(entity: Entity) -> EntityContainsColumnsValidator:
    validator = None
    for v in entity.get_validators():
        if isinstance(v, EntityContainsColumnsValidator):
            validator = v

    assert validator is not None
    validator.validation_mode = ColumnValidationMode.ERROR
    return validator


def test_mapped_columns_validation() -> None:
    entity = build_entity_from_config(CONFIG_PATH)

    query_entity = QueryEntity(entity.entity_key, entity.get_data_model())
    columns = [
        SelectedExpression(
            "ip_address", Column("_snuba_ip_address", None, "ip_address")
        ),
        SelectedExpression("email", Column("_snuba_email", None, "email")),
    ]

    bad_query = LogicalQuery(
        query_entity,
        selected_columns=columns
        + [SelectedExpression("asdf", Column("_snuba_asdf", None, "asdf"))],
    )

    good_query = LogicalQuery(query_entity, selected_columns=columns)
    validator = get_validator(entity)
    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)

    validator.validate(good_query)


def test_outcomes_columns_validation() -> None:
    entity = build_entity_from_config(CONFIG_PATH)

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
    validator = get_validator(entity)
    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)

    validator.validate(good_query)


def test_in_where_clause_and_function() -> None:
    entity = build_entity_from_config(CONFIG_PATH)

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
    validator = get_validator(entity)

    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)
