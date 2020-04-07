import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.transactions import TransactionsDataset
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.dsl import multiply
from snuba.query.logical import Query
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.request.request_settings import HTTPRequestSettings


tests = [
    (
        3600,
        FunctionCall(
            "my_start",
            "toStartOfHour",
            (Column(None, "start_ts", None), Literal(None, "Universal")),
        ),
        "(toStartOfHour(start_ts, 'Universal') AS my_start)",
    ),
    (
        60,
        FunctionCall(
            "my_start",
            "toStartOfMinute",
            (Column(None, "start_ts", None), Literal(None, "Universal")),
        ),
        "(toStartOfMinute(start_ts, 'Universal') AS my_start)",
    ),
    (
        86400,
        FunctionCall(
            "my_start",
            "toDate",
            (Column(None, "start_ts", None), Literal(None, "Universal")),
        ),
        "(toDate(start_ts, 'Universal') AS my_start)",
    ),
    (
        1440,
        FunctionCall(
            "my_start",
            "toDateTime",
            (
                multiply(
                    FunctionCall(
                        None,
                        "intDiv",
                        (
                            FunctionCall(
                                None, "toUInt32", (Column(None, "start_ts", None),),
                            ),
                            Literal(None, 1440),
                        ),
                    ),
                    Literal(None, 1440),
                ),
                Literal(None, "Universal"),
            ),
        ),
        "(toDateTime(multiply(intDiv(toUInt32(start_ts), 1440), 1440), 'Universal') AS my_start)",
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
            Column("transaction.duration", "duration", None),
            Column("my_start", "bucketed_start", None),
        ],
    )
    expected = Query(
        {"granularity": granularity},
        TableSource("transactions", ColumnSet([])),
        selected_columns=[Column("transaction.duration", "duration", None), ast_value,],
    )

    dataset = TransactionsDataset()
    TimeSeriesColumnProcessor(
        dataset._TimeSeriesDataset__time_group_columns
    ).process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == formatted_value
