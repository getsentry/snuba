import datetime
from typing import Optional

import pytest

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import ColumnValidationMode
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import (
    EntityContainsColumnsValidator,
    EntityRequiredColumnValidator,
)

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


@pytest.mark.parametrize("key, condition", tests)
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


@pytest.mark.parametrize("key, condition", invalid_tests)
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


def test_entity_contains_columns_valiator() -> None:
    key = EntityKey.EVENTS

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
                "timestamp", Column("_snuba_timestamp", None, "timestamp")
            ),
        ],
    )

    validator = EntityContainsColumnsValidator(
        entity.get_data_model(), validation_mode=ColumnValidationMode.ERROR
    )

    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)

    validator.validate(good_query)


def test_outcomes_columns_validation() -> None:
    key = EntityKey.OUTCOMES
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
            SelectedExpression("org_id", Column("_snuba_org_id", None, "org_id")),
            SelectedExpression(
                "project_id", Column("_snuba_project_id", None, "project_id")
            ),
            SelectedExpression("key_id", Column("_snuba_key_id", None, "key_id")),
            SelectedExpression(
                "timestamp", Column("_snuba_timestamp", None, "timestamp")
            ),
            SelectedExpression("outcome", Column("_snuba_outcome", None, "outcome")),
            SelectedExpression("reason", Column("_snuba_reason", None, "reason")),
            SelectedExpression("quantity", Column("_snuba_quantity", None, "quantity")),
            SelectedExpression("category", Column("_snuba_category", None, "category")),
            SelectedExpression(
                "times_seen", Column("_snuba_times_seen", None, "times_seen")
            ),
        ],
    )

    validator = EntityContainsColumnsValidator(
        entity.get_data_model(), validation_mode=ColumnValidationMode.ERROR
    )

    with pytest.raises(InvalidQueryException):
        validator.validate(bad_query)

    validator.validate(good_query)
