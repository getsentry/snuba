import datetime
import pytest
from typing import Optional

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import EntityRequiredColumnValidator

tests = [
    pytest.param(
        EntityKey.SPANS,
        binary_condition(
            "equals", Column("_snuba_project_id", None, "project_id"), Literal(None, 1),
        ),
        id="spans has project required with =",
    ),
    pytest.param(
        EntityKey.SPANS,
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


@pytest.mark.parametrize("key, condition", tests)  # type: ignore
def test_entity_validation(key: EntityKey, condition: Optional[Expression]) -> None:
    query = LogicalQuery(
        QueryEntity(key, get_entity(key).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    validator = EntityRequiredColumnValidator({"project_id"})
    validator.validate(query)


invalid_tests = [
    pytest.param(
        EntityKey.EVENTS, None, id="entity has columns, but there are no conditions",
    ),
    pytest.param(
        EntityKey.SPANS,
        binary_condition(
            "notIn",
            Column(None, None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        id="spans does not have project with EQ or IN",
    ),
]


@pytest.mark.parametrize("key, condition", invalid_tests)  # type: ignore
def test_entity_validation_failure(
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
