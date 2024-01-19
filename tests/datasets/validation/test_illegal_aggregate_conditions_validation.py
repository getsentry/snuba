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

conditions = [
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
        id="aggregate function in nested condition 2",
    ),
]


@pytest.mark.parametrize("condition", conditions)
def test_illegal_aggregate_in_condition_validator(
    condition: Optional[Expression],
) -> None:
    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )
    validator = IllegalAggregateInConditionValidator()
    with pytest.raises(InvalidQueryException):
        validator.validate(query)
