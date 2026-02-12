import datetime
from typing import Optional
from unittest.mock import MagicMock

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import DatetimeConditionValidator, logger

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
def test_datetime_column_validation(condition: Optional[Expression]) -> None:
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
            Literal(None, "2023-01-25T20:03:13+00:00"),
        ),
        "received AS `_snuba_received` requires datetime conditions: '2023-01-25T20:03:13+00:00' is not a valid datetime",
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
                    Literal(None, "2023-02-25T20:03:13+00:00"),
                ),
            ),
        ),
        "received AS `_snuba_received` requires datetime conditions: '2023-02-25T20:03:13+00:00' is not a valid datetime",
    ),
]


def test_invalid_datetime_column_validation() -> None:
    old_logger = logger.warning
    mock_logger = MagicMock()
    logger.warning = mock_logger  # type: ignore
    for condition, message in invalid_tests:
        query = LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=condition,
        )

        validator = DatetimeConditionValidator()
        validator.validate(query)

        mock_logger.assert_called_with(message)
        mock_logger.reset_mock()

    logger.warning = old_logger  # type: ignore
