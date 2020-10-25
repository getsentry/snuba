from snuba.clickhouse.columns import Any, ColumnSet, String
from snuba.datasets.entities import EntityKey
from snuba.query import Query, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column
from snuba.query.logical import Query as LogicalQuery


def test_nested_query() -> None:
    """
    Simply builds a nested query.
    """

    nested = LogicalQuery(
        {},
        Entity(EntityKey.EVENTS, ColumnSet([("event_id", String())])),
        selected_columns=[
            SelectedExpression(
                "string_evt_id", Column("string_evt_id", None, "event_id")
            )
        ],
    )

    composite = Query[LogicalQuery](
        from_clause=nested,
        selected_columns=[
            SelectedExpression("output", Column("output", None, "string_evt_id"))
        ],
    )

    # The iterator methods on the composite query do not descend into
    # the nested query
    assert composite.get_all_ast_referenced_columns() == {
        Column("output", None, "string_evt_id")
    }

    # The schema of the nested query is the selected clause of that query.
    assert composite.get_from_clause().get_columns() == ColumnSet(
        [("string_evt_id", Any())]
    )
