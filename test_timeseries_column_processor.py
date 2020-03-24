import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.transactions import TransactionsDataset
from snuba.query.conditions import (
	binary_condition,
	BooleanFunctions,
	ConditionFunctions,
)
from snuba.query.dsl import div, multiply, plus
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings

tests = [
	(3600, "toStartOfHour(start_ts)"),
	(60, "toStartOfMinute(start_ts)"),
	(86400, "toDate(start_ts)"),
	(1440, "toDateTime(intDiv(toUInt32(start_ts), 1440) * 1440)"),
]

@pytest.mark.parametrize("granularity, expected_column_name", tests)
def test_timeseries_column_format_expressions(granularity, expected_column_name) -> None:
	unprocessed = Query(
		{"granularity": granularity},
		TableSource("transactions", ColumnSet([])),
		selected_columns=[
			Column("transaction.duration", "duration", None),
			Column(None, "bucketed_start", None),
		],
	)
	expected = Query(
		{"granularity": granularity},
		TableSource("transactions", ColumnSet([])),
		selected_columns=[
			Column("transaction.duration", "duration", None),
			Column(None, expected_column_name, None),
		],
	)

	dataset = TransactionsDataset()
	print(dir(dataset))
	TimeSeriesColumnProcessor(dataset._TimeSeriesDataset__time_group_columns).process_query(unprocessed, HTTPRequestSettings())
	assert (
		expected.get_selected_columns_from_ast()
		== unprocessed.get_selected_columns_from_ast()
	)