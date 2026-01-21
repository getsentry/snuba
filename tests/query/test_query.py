from itertools import chain
from unittest.mock import MagicMock, patch

from snuba.clickhouse.columns import Any, ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import LimitBy, SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall


def test_query_parameters() -> None:
    query = Query(
        Table("my_table", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        limitby=LimitBy(
            100, [Column(alias=None, table_name="my_table", column_name="environment")]
        ),
        limit=100,
        offset=50,
        totals=True,
        granularity=60,
    )
    assert query.get_limitby() == LimitBy(
        100, [Column(alias=None, table_name="my_table", column_name="environment")]
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
        Table("my_table", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[
            SelectedExpression("col1", Column(alias="col1", table_name=None, column_name="col1")),
            SelectedExpression(
                "some_func",
                FunctionCall(
                    "some_func",
                    "f",
                    (Column(alias="col1", table_name=None, column_name="col1"),),
                ),
            ),
            SelectedExpression(None, Column(alias="col2", table_name=None, column_name="col2")),
        ],
    )
    assert query.get_columns() == ColumnSet(
        [("col1", Any()), ("some_func", Any()), ("_invalid_alias_2", Any())]
    )


def test_query_experiments() -> None:
    query = Query(
        Table("my_table", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        limitby=LimitBy(
            100, [Column(alias=None, table_name="my_table", column_name="environment")]
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


@patch("snuba.query.replace")
def test_query_transformation(mock_replace: MagicMock) -> None:
    """
    We want to verify that calling .transform on a Query with a visitor
    will apply said visitor to all expressions (expression.accept(visitor))
    that are in the query.
    """

    # Transfrom will call replace, we need to mock it since our "expressions"
    # in this tests are Magic Mocks and not real expressions.
    mock_replace.return_value = None

    mock_visitor = MagicMock()

    mock_selected_columns = [MagicMock()]
    mock_array_join = [MagicMock()]
    mock_condition = MagicMock()
    mock_groupby = [MagicMock()]
    mock_having = MagicMock()
    mock_order_by = [MagicMock()]
    mock_limitby = MagicMock()
    query = Query(
        Table("my_table", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=mock_selected_columns,
        array_join=mock_array_join,
        condition=mock_condition,
        groupby=mock_groupby,
        having=mock_having,
        order_by=mock_order_by,
        limitby=mock_limitby,
    )

    query.transform(mock_visitor)

    for mock_clause in chain(mock_selected_columns, mock_order_by):
        # Verfiy that all expressions within clauses have accepted our visitor
        mock_clause.expression.accept.assert_called_once_with(mock_visitor)

    for mock_expr in chain(
        mock_array_join, [mock_condition], mock_groupby, [mock_having], mock_limitby
    ):
        # Verfiy that all other expressions have accepted our visitor
        mock_expr.accept.assert_called_once_with(mock_visitor)
