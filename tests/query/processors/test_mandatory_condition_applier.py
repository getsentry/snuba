import copy
from typing import List

import pytest

from snuba.datasets.schemas import MandatoryCondition
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        "table1",
        [
            MandatoryCondition(
                ["deleted", "=", "0"],
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column("deleted", None, "deleted"),
                    Literal(None, "0"),
                ),
            )
        ],
    ),
    (
        "table2",
        [
            MandatoryCondition(
                ["time", "=", "1"],
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column("time", None, "time"),
                    Literal(None, "1"),
                ),
            ),
            MandatoryCondition(
                ["time2", "=", "2"],
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column("time2", None, "time2"),
                    Literal(None, "2"),
                ),
            ),
        ],
    ),
]


@pytest.mark.parametrize("table, mand_condition", test_data)
def test_mand_condition(table: str, mand_condition: List[MandatoryCondition]) -> None:

    body = {
        "conditions": [
            ["d", "=", "1"],
            ["c", "=", "3"],
            ["a", "=", "1"],
            ["b", "=", "2"],
        ],
    }

    body_copy = copy.deepcopy(body)

    cols = None
    consistent = True

    query = Query(
        body_copy,
        TableSource(table, cols, mand_condition, ["c1"]),
        None,
        None,
        binary_condition(
            None,
            BooleanFunctions.AND,
            binary_condition(
                None,
                BooleanFunctions.AND,
                binary_condition(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    Column("d", None, "d"),
                    Literal(None, "1"),
                ),
                binary_condition(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    Column("c", None, "c"),
                    Literal(None, "3"),
                ),
            ),
            binary_condition(
                None,
                BooleanFunctions.AND,
                binary_condition(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    Column("a", None, "a"),
                    Literal(None, "1"),
                ),
                binary_condition(
                    None,
                    OPERATOR_TO_FUNCTION["="],
                    Column("b", None, "b"),
                    Literal(None, "2"),
                ),
            ),
        ),
    )

    query_ast_copy = copy.deepcopy(query)

    request_settings = HTTPRequestSettings(consistent=consistent)
    processor = MandatoryConditionApplier()
    processor.process_query(query, request_settings)

    body["conditions"].extend([c.legacy for c in mand_condition])
    assert query.get_conditions() == body["conditions"]

    query_ast_copy.add_condition_to_ast(
        combine_and_conditions([c.ast for c in mand_condition])
    )

    assert query.get_condition_from_ast() == query_ast_copy.get_condition_from_ast()
