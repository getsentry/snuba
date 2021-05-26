import pytest

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import OneProjectValidator

tests = [
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "equals", Column("_snuba_project_id", None, "project_id"), Literal(None, 1),
        ),
        id="equals",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "in",
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        id="in",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "in",
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1), Literal(None, 1))),
        ),
        id="dumb in",
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
                Column("_snuba_project_id", None, "project_id"),
                FunctionCall(None, "tuple", (Literal(None, 1),)),
            ),
        ),
        id="multiple conditions for the same project",
    ),
]


@pytest.mark.parametrize("key, condition", tests)  # type: ignore
def test_one_project_validation(key: EntityKey, condition: Expression) -> None:
    query = LogicalQuery(
        QueryEntity(key, get_entity(key).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    validator = OneProjectValidator()
    validator.validate(query)


invalid_tests = [
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "in",
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1), Literal(None, 2))),
        ),
        id="more than one",
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
                Column("_snuba_project_id", None, "project_id"),
                FunctionCall(None, "tuple", (Literal(None, 2),)),
            ),
        ),
        id="multiple conditions for different projects",
    ),
]


@pytest.mark.parametrize("key, condition", invalid_tests)  # type: ignore
def test_one_project_validation_failure(key: EntityKey, condition: Expression) -> None:
    query = LogicalQuery(
        QueryEntity(key, get_entity(key).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    validator = OneProjectValidator()
    with pytest.raises(InvalidQueryException):
        validator.validate(query)
