from snuba.clickhouse.columns import Any, ColumnSet
from snuba.clickhouse.query import Query
from snuba.query import LimitBy, SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall


def test_query_parameters() -> None:
    query = Query(
        Table("my_table", ColumnSet([])),
        limitby=LimitBy(
            100, Column(alias=None, table_name="my_table", column_name="environment")
        ),
        limit=100,
        offset=50,
        totals=True,
        granularity=60,
    )
    assert query.get_limitby() == LimitBy(
        100, Column(alias=None, table_name="my_table", column_name="environment")
    )
    assert query.get_limit() == 100
    assert query.get_offset() == 50
    assert query.has_totals() is True
    assert query.get_granularity() == 60

    assert query.get_from_clause().table_name == "my_table"


def test_query_data_source() -> None:
    """
    Tests using the Query as a data source
    """

    query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "col1", Column(alias="col1", table_name=None, column_name="col1")
            ),
            SelectedExpression(
                "some_func",
                FunctionCall(
                    "some_func",
                    "f",
                    (Column(alias="col1", table_name=None, column_name="col1"),),
                ),
            ),
            SelectedExpression(
                None, Column(alias="col2", table_name=None, column_name="col2")
            ),
        ],
    )
    assert query.get_columns() == ColumnSet(
        [("col1", Any()), ("some_func", Any()), ("_invalid_alias_2", Any())]
    )


def test_query_experiments() -> None:
    query = Query(
        Table("my_table", ColumnSet([])),
        limitby=LimitBy(
            100, Column(alias=None, table_name="my_table", column_name="environment")
        ),
        limit=100,
        offset=50,
        granularity=60,
    )

    query.set_experiments({"optimization1": True})
    assert query.get_experiments() == {"optimization1": True}
    assert query.get_experiment_value("optimization1") == True

    assert query.get_experiment_value("optimization2") is None
    query.add_experiment("optimization2", "group1")
    assert query.get_experiment_value("optimization2") == "group1"

    query.set_experiments({"optimization3": 0.5})
    assert query.get_experiments() == {"optimization3": 0.5}
