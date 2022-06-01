from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.type_converters.uuid_column_processor import (
    UUIDColumnProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings


def test_event_id_column_format_expressions() -> None:
    unprocessed = Query(
        Table("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression(
                "the_event_id", Column("the_event_id", None, "event_id")
            ),
        ],
    )
    expected = Query(
        Table("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression(
                "the_event_id",
                FunctionCall(
                    "the_event_id",
                    "replaceAll",
                    (
                        FunctionCall(
                            None,
                            "toString",
                            (Column(None, None, "event_id"),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
            ),
        ],
    )

    UUIDColumnProcessor({"event_id"}).process_query(unprocessed, HTTPQuerySettings())
    assert expected.get_selected_columns() == unprocessed.get_selected_columns()

    formatted = unprocessed.get_selected_columns()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert formatted == "(replaceAll(toString(event_id), '-', '') AS the_event_id)"
