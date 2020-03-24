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
from snuba.query.processors.transaction_column_processor import TransactionColumnProcessor
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings


def test_transaction_column_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("transaction.duration", "duration", None),
            Column("v4_ip_address", "ip_address_v4", None),
            Column(None, "ip_address_v6", None),
            Column(None, "ip_address", None),
            Column(None, "event_id", None),
            Column(None, "tags[user.email]", None),
            Column(None, "contexts[device.simulator]", None),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("transaction.duration", "duration", None),
            Column("v4_ip_address", "IPv4NumToString(ip_address_v4)", None),
            Column(None, "IPv6NumToString(ip_address_v6)", None),
            Column(None, "coalesce(IPv4NumToString(ip_address_v4), IPv6NumToString(ip_address_v6))", None),
            Column(None, "replaceAll(toString(event_id), '-', '')", None),
            Column(None, "tags.value[indexOf(tags.key, 'user.email')]", None),
            Column(None, "contexts.value[indexOf(contexts.key, 'device.simulator')]", None),
        ],
    )

    dataset = TransactionsDataset()
    TransactionColumnProcessor(dataset._TransactionsDataset__tags_processor).process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )