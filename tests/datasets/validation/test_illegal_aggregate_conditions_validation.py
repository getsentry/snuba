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
from snuba.query.validation.validators import IllegalAggregateInConditionValidator

tests = [
    pytest.param(
        FunctionCall(
            None,
            "greater",
            (
                FunctionCall(
                    None,
                    "sumIf",
                    (
                        Column(None, None, "duration"),
                        FunctionCall(
                            None,
                            "equals",
                            (Column(None, None, "is_segment"), Literal(None, 1)),
                        ),
                    ),
                ),
                Literal(None, 42),
            ),
        ),
        None,
        InvalidQueryException,
        id="aggregate function in top level",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_received", None, "received"),
                Literal(None, datetime.datetime(2023, 1, 25, 20, 3, 13)),
            ),
            FunctionCall(
                None,
                "less",
                (
                    FunctionCall(
                        None,
                        "countIf",
                        (
                            Column(None, None, "duration"),
                            FunctionCall(
                                None,
                                "equals",
                                (Column(None, None, "is_segment"), Literal(None, 1)),
                            ),
                        ),
                    ),
                    Literal(None, 42),
                ),
            ),
        ),
        None,
        InvalidQueryException,
        id="aggregate function in nested condition 1",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_received", None, "received"),
                Literal(None, datetime.datetime(2023, 1, 25, 20, 3, 13)),
            ),
            FunctionCall(
                None,
                "less",
                (
                    FunctionCall(
                        None,
                        "max",
                        (Column(None, None, "duration"),),
                    ),
                    Literal(None, 42),
                ),
            ),
        ),
        None,
        InvalidQueryException,
        id="aggregate function in nested condition 2",
    ),
    pytest.param(
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_received", None, "received"),
                Literal(None, datetime.datetime(2023, 1, 25, 20, 3, 13)),
            ),
            FunctionCall(
                None,
                "less",
                (
                    Column("_snuba_group_id", None, "group_id"),
                    Literal(None, 2),
                ),
            ),
        ),
        FunctionCall(
            None,
            "less",
            (
                FunctionCall(
                    None,
                    "max",
                    (Column(None, None, "duration"),),
                ),
                Literal(None, 42),
            ),
        ),
        None,
        id="no aggregate function in where clause but in having clause",
    ),
]


@pytest.mark.parametrize("condition, having, exception", tests)
@pytest.mark.redis_db
def test_illegal_aggregate_in_condition_validator(
    condition: Optional[Expression],
    having: Optional[Expression],
    exception: Exception,
) -> None:
    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
        having=having,
    )
    validator = IllegalAggregateInConditionValidator()
    if exception:
        with pytest.raises(exception):
            validator.validate(query)
    else:
        validator.validate(query)
