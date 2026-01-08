import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import NoTimeBasedConditionValidator

tests = [
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "equals",
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        id="equals",
    ),
]


@pytest.mark.parametrize("key, condition", tests)  # type: ignore
def test_no_time_based_validation(key: EntityKey, condition: Expression) -> None:
    entity = get_entity(key)
    query = LogicalQuery(
        QueryEntity(key, entity.get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    assert entity.required_time_column is not None
    validator = NoTimeBasedConditionValidator(entity.required_time_column)
    validator.validate(query)


invalid_tests = [
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "equals",
            Column("_snuba_timestamp", None, "timestamp"),
            Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
        ),
        id="equals",
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
                "greater",
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        id="greater",
    ),
]


@pytest.mark.parametrize("key, condition", invalid_tests)  # type: ignore
def test_no_time_based_validation_failure(
    key: EntityKey, condition: Expression
) -> None:
    entity = get_entity(key)
    query = LogicalQuery(
        QueryEntity(key, entity.get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    assert entity.required_time_column is not None
    validator = NoTimeBasedConditionValidator(entity.required_time_column)
    with pytest.raises(InvalidQueryException):
        validator.validate(query)
