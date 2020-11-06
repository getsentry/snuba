import copy
from typing import List

import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    pytest.param(
        "table1",
        [
            binary_condition(
                None,
                ConditionFunctions.EQ,
                Column("deleted", None, "deleted"),
                Literal(None, "0"),
            ),
        ],
        id="Single Mandatory Condition TestCase",
    ),
    pytest.param(
        "table2",
        [
            binary_condition(
                None,
                ConditionFunctions.EQ,
                Column("time", None, "time"),
                Literal(None, "1"),
            ),
            binary_condition(
                None,
                ConditionFunctions.EQ,
                Column("time2", None, "time2"),
                Literal(None, "2"),
            ),
        ],
        id="Multiple Mandatory Condition TestCase",
    ),
]


@pytest.mark.parametrize("table, mand_conditions", test_data)
def test_mand_conditions(table: str, mand_conditions: List[FunctionCall]) -> None:

    query = Query(
        Table(
            table,
            ColumnSet([]),
            final=False,
            sampling_rate=None,
            mandatory_conditions=mand_conditions,
            prewhere_candidates=["c1"],
        ),
        None,
        None,
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
    )

    query_ast_copy = copy.deepcopy(query)

    request_settings = HTTPRequestSettings(consistent=True)
    processor = MandatoryConditionApplier()
    processor.process_query(query, request_settings)

    query_ast_copy.add_condition_to_ast(combine_and_conditions(mand_conditions))

    assert query.get_condition_from_ast() == query_ast_copy.get_condition_from_ast()
