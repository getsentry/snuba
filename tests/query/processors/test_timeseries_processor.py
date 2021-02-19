from typing import Optional

import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.transactions import TransactionsEntity
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import multiply
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.timeseries_processor import (
    TimeSeriesProcessor,
    extract_granularity_from_query,
)
from snuba.request.request_settings import HTTPRequestSettings
from snuba.util import parse_datetime

tests = [
    pytest.param(
        3600,
        binary_condition(
            ConditionFunctions.EQ,
            Column("my_time", None, "time"),
            Literal(None, "2020-01-01"),
        ),
        FunctionCall(
            "my_time",
            "toStartOfHour",
            (Column(None, None, "finish_ts"), Literal(None, "Universal")),
        ),
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                "my_time",
                "toStartOfHour",
                (Column(None, None, "finish_ts"), Literal(None, "Universal")),
            ),
            Literal(None, parse_datetime("2020-01-01")),
        ),
        "(toStartOfHour(finish_ts, 'Universal') AS my_time)",
        "equals((toStartOfHour(finish_ts, 'Universal') AS my_time), toDateTime('2020-01-01T00:00:00', 'Universal'))",
        id="granularity-3600-simple-condition",
    ),
    pytest.param(
        60,
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column("my_time", None, "time"),
                Literal(None, "2020-01-01"),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "transaction"),
                Literal(None, "something"),
            ),
        ),
        FunctionCall(
            "my_time",
            "toStartOfMinute",
            (Column(None, None, "finish_ts"), Literal(None, "Universal")),
        ),
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    "my_time",
                    "toStartOfMinute",
                    (Column(None, None, "finish_ts"), Literal(None, "Universal")),
                ),
                Literal(None, parse_datetime("2020-01-01")),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "transaction"),
                Literal(None, "something"),
            ),
        ),
        "(toStartOfMinute(finish_ts, 'Universal') AS my_time)",
        "equals((toStartOfMinute(finish_ts, 'Universal') AS my_time), toDateTime('2020-01-01T00:00:00', 'Universal')) AND equals(transaction, 'something')",
        id="granularity-60-condition-on-non-time-column",
    ),
    pytest.param(
        86400,
        None,
        FunctionCall(
            "my_time",
            "toDate",
            (Column(None, None, "finish_ts"), Literal(None, "Universal")),
        ),
        None,
        "(toDate(finish_ts, 'Universal') AS my_time)",
        "",
        id="granularity-86400",
    ),
    pytest.param(
        1440,
        None,
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
        None,
        "(toDateTime(multiply(intDiv(toUInt32(finish_ts), 1440), 1440), 'Universal') AS my_time)",
        "",
        id="granularity-1440",
    ),
]


@pytest.mark.parametrize(
    "granularity, condition, exp_column, exp_condition, formatted_column, formatted_condition",
    tests,
)
def test_timeseries_format_expressions(
    granularity: int,
    condition: Optional[FunctionCall],
    exp_column: FunctionCall,
    exp_condition: Optional[FunctionCall],
    formatted_column: str,
    formatted_condition: str,
) -> None:
    unprocessed = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression("my_time", Column("my_time", None, "time")),
        ],
        condition=condition,
        groupby=[Column("my_time", None, "time")],
        granularity=granularity,
    )
    expected = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression(exp_column.alias, exp_column),
        ],
        condition=exp_condition,
    )

    entity = TransactionsEntity()
    processors = entity.get_query_processors()
    for processor in processors:
        if isinstance(processor, TimeSeriesProcessor):
            processor.process_query(unprocessed, HTTPRequestSettings())

    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )
    assert expected.get_condition_from_ast() == unprocessed.get_condition_from_ast()

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == formatted_column
    if condition:
        ret = unprocessed.get_condition_from_ast().accept(
            ClickhouseExpressionFormatter()
        )
        assert formatted_condition == ret

    assert extract_granularity_from_query(unprocessed, "finish_ts") == granularity


def test_invalid_datetime() -> None:
    unprocessed = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
        ],
        condition=binary_condition(
            ConditionFunctions.EQ, Column("my_time", None, "time"), Literal(None, ""),
        ),
    )

    entity = TransactionsEntity()
    processors = entity.get_query_processors()
    for processor in processors:
        if isinstance(processor, TimeSeriesProcessor):
            with pytest.raises(InvalidQueryException):
                processor.process_query(unprocessed, HTTPRequestSettings())
