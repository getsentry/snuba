import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.discover import DiscoverDataset
from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.dsl import div, multiply, plus
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.discover_column_processor import DiscoverColumnProcessor
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings


def test_discover_column_format_events_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("t_time", "time", None),
            Column("t_release", "release", None),
            Column("t_dist", "dist", None),
            Column("t_user", "user", None),
            Column("t_transaction_hash", "transaction_hash", None),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("t_time", "timestamp", None),
            Column("t_release", "tags[sentry:release]", None),
            Column("t_dist", "tags[sentry:dist]", None),
            Column("t_user", "tags[sentry:user]", None),
            Column("t_transaction_hash", "NULL", None),
        ],
    )

    def mock_detect_table(*args, **kwargs):
        return "events"

    dataset = DiscoverDataset()
    DiscoverColumnProcessor(
        dataset._DiscoverDataset__events_columns,
        dataset._DiscoverDataset__transactions_columns,
        mock_detect_table,
    ).process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    expected = [
        "(timestamp AS t_time)",
        "(`tags[sentry:release]` AS t_release)",
        "(`tags[sentry:dist]` AS t_dist)",
        "(`tags[sentry:user]` AS t_user)",
        "(NULL AS t_transaction_hash)",
    ]
    columns = unprocessed.get_selected_columns_from_ast()
    for idx, col in enumerate(columns):
        ret = col.accept(ClickhouseExpressionFormatter())
        assert ret == expected[idx]


def test_discover_column_format_transactions_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("transactions", ColumnSet([])),
        selected_columns=[
            Column(None, "time", None),
            Column(None, "type", None),
            Column(None, "timestamp", None),
            Column(None, "username", None),
            Column(None, "email", None),
            Column(None, "transaction", None),
            Column(None, "message", None),
            Column(None, "title", None),
            Column(None, "group_id", None),
            Column(None, "geo_country_code", None),
            Column(None, "geo_region", None),
            Column(None, "geo_city", None),
            Column(None, "server_name", None),
        ],
    )
    expected = Query(
        {},
        TableSource("transactions", ColumnSet([])),
        selected_columns=[
            Column(None, "finish_ts", None),
            Column(None, "'transaction'", None),
            Column(None, "finish_ts", None),
            Column(None, "user_name", None),
            Column(None, "user_email", None),
            Column(None, "transaction_name", None),
            Column(None, "transaction_name", None),
            Column(None, "transaction_name", None),
            Column(None, "0", None),
            Column(None, "contexts[geo.country_code]", None),
            Column(None, "contexts[geo.region]", None),
            Column(None, "contexts[geo.city]", None),
            Column(None, "NULL", None),
        ],
    )

    def mock_detect_table(*args, **kwargs):
        return "transactions"

    dataset = DiscoverDataset()
    DiscoverColumnProcessor(
        dataset._DiscoverDataset__events_columns,
        dataset._DiscoverDataset__transactions_columns,
        mock_detect_table,
    ).process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )
