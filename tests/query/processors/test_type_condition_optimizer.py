from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Literal
from snuba.query.processors.physical.type_condition_optimizer import (
    TypeConditionOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings


def test_type_condition_optimizer() -> None:
    cond1 = binary_condition(
        ConditionFunctions.EQ, Column(None, None, "col1"), Literal(None, "val1")
    )

    unprocessed_query = Query(
        Table("errors", ColumnSet([]), storage_key=StorageKey("errors")),
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
        Table("errors", ColumnSet([]), storage_key=StorageKey("errors")),
        condition=binary_condition(BooleanFunctions.AND, Literal(None, 1), cond1),
    )
    TypeConditionOptimizer().process_query(unprocessed_query, HTTPQuerySettings())

    assert expected_query.get_condition() == unprocessed_query.get_condition()
    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == "1 AND equals(col1, 'val1')"
