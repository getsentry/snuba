import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.transactions import TransactionsDataset
from snuba.query.dsl import multiply
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.request.request_settings import HTTPRequestSettings

tests = [
    (
        3600,
        FunctionCall(
            "my_time",
            "toStartOfHour",
            (Column(None, None, "finish_ts"), Literal(None, "Universal")),
        ),
        "(toStartOfHour(finish_ts, 'Universal') AS my_time)",
    ),
    (
        60,
        FunctionCall(
            "my_time",
            "toStartOfMinute",
            (Column(None, None, "finish_ts"), Literal(None, "Universal")),
        ),
        "(toStartOfMinute(finish_ts, 'Universal') AS my_time)",
    ),
    (
        86400,
        FunctionCall(
            "my_time",
            "toDate",
            (Column(None, None, "finish_ts"), Literal(None, "Universal")),
        ),
        "(toDate(finish_ts, 'Universal') AS my_time)",
    ),
    (
        1440,
        FunctionCall(
            "my_time",
            "toDateTime",
            (
                multiply(
                    FunctionCall(
                        None,
                        "intDiv",
                        (
                            FunctionCall(
                                None, "toUInt32", (Column(None, None, "finish_ts"),),
                            ),
                            Literal(None, 1440),
                        ),
                    ),
                    Literal(None, 1440),
                ),
                Literal(None, "Universal"),
            ),
        ),
        "(toDateTime(multiply(intDiv(toUInt32(finish_ts), 1440), 1440), 'Universal') AS my_time)",
    ),
]


@pytest.mark.parametrize("granularity, ast_value, formatted_value", tests)
def test_timeseries_column_format_expressions(
    granularity, ast_value, formatted_value
) -> None:
    unprocessed = Query(
        {"granularity": granularity},
        TableSource("transactions", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression("my_time", Column("my_time", None, "time")),
        ],
    )
    expected = Query(
        {"granularity": granularity},
        TableSource("transactions", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression(ast_value.alias, ast_value),
        ],
    )

    dataset = TransactionsDataset()
    TimeSeriesColumnProcessor(
        dataset._TimeSeriesDataset__time_group_columns
    ).process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == formatted_value
