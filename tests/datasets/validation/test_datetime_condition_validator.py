import datetime
import re

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import DatetimeConditionValidator

required_column_tests = [
    pytest.param(
        binary_condition(
            "equals",
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        id="No datetime column condition means no validation",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_received", None, "received"),
                Literal(None, datetime.datetime(2023, 1, 25, 20, 3, 13)),
            ),
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
        ),
        id="valid condition works fine",
    ),
]


@pytest.mark.parametrize("condition", required_column_tests)
def test_datetime_column_validation(condition: Expression | None) -> None:
    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )
    validator = DatetimeConditionValidator()
    validator.validate(query)


invalid_tests = [
    (
        binary_condition(
            "equals",
            Column("_snuba_received", None, "received"),
            # A bare epoch (here in milliseconds) is not a valid datetime.
            Literal(None, "1726374214000"),
        ),
        "received AS `_snuba_received` requires datetime conditions: '1726374214000' is not a valid datetime",
    ),
    (
        binary_condition(
            "in",
            Column("_snuba_received", None, "received"),
            FunctionCall(
                None,
                "array",
                (
                    Literal(None, datetime.datetime.utcnow()),
                    Literal(None, "not a datetime"),
                ),
            ),
        ),
        "received AS `_snuba_received` requires datetime conditions: 'not a datetime' is not a valid datetime",
    ),
]


valid_datetime_string_tests = [
    pytest.param(
        binary_condition(
            "equals",
            Column("_snuba_received", None, "received"),
            Literal(None, "2023-01-25T20:03:13+00:00"),
        ),
        id="ISO datetime string with timezone",
    ),
    pytest.param(
        binary_condition(
            "equals",
            Column("_snuba_received", None, "received"),
            # str(datetime), as emitted by the legacy JSON query API.
            Literal(None, "2023-01-25 20:03:13"),
        ),
        id="space-separated datetime string",
    ),
    pytest.param(
        binary_condition(
            "in",
            Column("_snuba_received", None, "received"),
            FunctionCall(
                None,
                "array",
                (
                    Literal(None, datetime.datetime.utcnow()),
                    Literal(None, "2023-02-25T20:03:13+00:00"),
                ),
            ),
        ),
        id="array of datetime and datetime string",
    ),
]


@pytest.mark.parametrize("condition", valid_datetime_string_tests)
def test_valid_datetime_string_conditions(condition: Expression) -> None:
    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    DatetimeConditionValidator().validate(query)


def test_invalid_datetime_column_validation() -> None:
    for condition, message in invalid_tests:
        query = LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=condition,
        )

        validator = DatetimeConditionValidator()
        with pytest.raises(InvalidQueryException, match=re.escape(message)):
            validator.validate(query)
