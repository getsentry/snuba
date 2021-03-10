from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    ConditionFunctions,
    binary_condition,
    BooleanFunctions,
)
from snuba.query.data_source.simple import Table
from snuba.datasets.storages.type_condition_optimizer import TypeConditionOptimizer
from snuba.request.request_settings import HTTPRequestSettings
from snuba.query.expressions import Column, Literal
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter


def test_type_condition_optimizer() -> None:
    cond1 = binary_condition(
        ConditionFunctions.EQ, Column(None, None, "col1"), Literal(None, "val1")
    )

    unprocessed_query = Query(
        Table("errors", ColumnSet([])),
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.NEQ,
                Column(None, None, "type"),
                Literal(None, "transaction"),
            ),
            cond1,
        ),
    )
    expected_query = Query(
        Table("errors", ColumnSet([])),
        condition=binary_condition(BooleanFunctions.AND, Literal(None, 1), cond1),
    )
    TypeConditionOptimizer().process_query(unprocessed_query, HTTPRequestSettings())

    assert (
        expected_query.get_condition_from_ast()
        == unprocessed_query.get_condition_from_ast()
    )
    condition = unprocessed_query.get_condition_from_ast()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == "1 AND equals(col1, 'val1')"
