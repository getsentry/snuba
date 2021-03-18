import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    ConditionFunctions,
    binary_condition,
    BooleanFunctions,
)
from snuba.query.expressions import FunctionCall
from snuba.query.data_source.simple import Table
from snuba.datasets.storages.type_condition_enforcer import TypeConditionEnforcer
from snuba.request.request_settings import HTTPRequestSettings
from snuba.query.expressions import Column, Literal
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter


cond1 = binary_condition(
    ConditionFunctions.EQ, Column(None, None, "col1"), Literal(None, "val1")
)


test_data = [
    pytest.param(
        Query(
            Table("events", ColumnSet([])),
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.NEQ,
                    Column(None, None, "type"),
                    Literal(None, "transaction"),
                ),
                cond1,
            ),
        ),
        "notEquals(type, 'transaction') AND equals(col1, 'val1')",
        id="has_type_condition",
    ),
    pytest.param(
        Query(
            Table("events", ColumnSet([])),
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.IN,
                    Column(None, None, "group_id"),
                    FunctionCall(None, "tuple", (Literal(None, 1), Literal(None, 2))),
                ),
                cond1,
            ),
        ),
        "in(group_id, tuple(1, 2)) AND equals(col1, 'val1')",
        id="has_groupid_condition",
    ),
    pytest.param(
        Query(Table("events", ColumnSet([])), condition=cond1,),
        "notEquals((type AS _snuba_type), 'transaction') AND equals(col1, 'val1')",
        id="no_type_or_group_condition",
    ),
]


@pytest.mark.parametrize("query, formatted", test_data)
def test_type_condition_optimizer(query: Query, formatted: str) -> None:
    TypeConditionEnforcer().process_query(query, HTTPRequestSettings())
    condition = query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted
