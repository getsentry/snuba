import datetime
from typing import Optional

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import (
    ColumnValidationMode,
    EntityContainsColumnsValidator,
    EntityRequiredColumnValidator,
)

required_column_tests = [
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "equals",
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        id="spans has project required with =",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "in",
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        id="in is also allowed",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "equals",
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        id="specific time conditions are valid",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "in",
                Column("_snuba_timestamp", None, "timestamp"),
                FunctionCall(
                    None, "tuple", (Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),)
                ),
            ),
        ),
        id="filtering time with IN is allowed",
    ),
]


@pytest.mark.parametrize("key, condition", required_column_tests)
def test_entity_required_column_validation(
    key: EntityKey, condition: Optional[Expression]
) -> None:
    query = LogicalQuery(
        QueryEntity(key, get_entity(key).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    validator = EntityRequiredColumnValidator({"project_id"})
    validator.validate(query)


invalid_required_column_tests = [
    pytest.param(
        EntityKey.EVENTS,
        None,
        id="entity has columns, but there are no conditions",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "notIn",
            Column(None, None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        id="spans does not have project with EQ or IN",
    ),
]


@pytest.mark.parametrize("key, condition", invalid_required_column_tests)
def test_entity_required_column_validation_failure(
    key: EntityKey, condition: Optional[Expression]
) -> None:
    query = LogicalQuery(
        QueryEntity(key, get_entity(key).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    validator = EntityRequiredColumnValidator({"project_id"})
    with pytest.raises(InvalidQueryException):
        validator.validate(query)


entity_contains_columns_tests = [
    pytest.param(
        EntityKey.OUTCOMES,
        id="Validate Outcomes Entity Columns",
    )
]


@pytest.mark.parametrize("key", entity_contains_columns_tests)
def test_outcomes_columns_validation(key: EntityKey) -> None:
    entity = get_entity(key)

    query_entity = QueryEntity(key, entity.get_data_model())

    bad_query = LogicalQuery(
        query_entity,
        selected_columns=[
            SelectedExpression("asdf", Column("_snuba_asdf", None, "asdf")),
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
