import datetime
import re
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
from snuba.query.validation.validators import DatetimeConditionValidator

required_column_tests = [
    pytest.param(
        binary_condition(
            "equals",
            Column("_snuba_project_id", None, "project_id"),
            Literal(None, 1),
        ),
        None,
        id="No datetime column condition means no validation",
    ),
    pytest.param(
        binary_condition(
            "equals",
            Column("_snuba_received", None, "received"),
            Literal(None, "2023-01-25T20:03:13+00:00"),
        ),
        InvalidQueryException(
            "received AS `_snuba_received` requires datetime conditions: '2023-01-25T20:03:13+00:00' is not a valid datetime"
        ),
        id="datetime column with non-datetime literal",
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
                    Literal(None, "2023-01-25T20:03:13+00:00"),
                ),
            ),
        ),
        InvalidQueryException(
            "received AS `_snuba_received` requires datetime conditions: '2023-01-25T20:03:13+00:00' is not a valid datetime"
        ),
        id="datetime column with non-datetime literal in array",
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
        None,
        id="valid condition works fine",
    ),
]


@pytest.mark.parametrize("condition, exception", required_column_tests)
def test_datetime_column_validation(
    condition: Optional[Expression], exception: Optional[Exception]
) -> None:
    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    validator = DatetimeConditionValidator()
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validator.validate(query)
    else:
        validator.validate(query)
