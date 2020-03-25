from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.transaction_column_processor import (
    TransactionColumnProcessor,
)
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
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("transaction.duration", "duration", None),
            FunctionCall(
                "v4_ip_address",
                "IPv4NumToString",
                (Column(None, "ip_address_v4", None),),
            ),
            FunctionCall(
                None, "IPv6NumToString", (Column(None, "ip_address_v6", None),),
            ),
            FunctionCall(
                None,
                "coalesce",
                (
                    FunctionCall(
                        None, "IPv4NumToString", (Column(None, "ip_address_v4", None),),
                    ),
                    FunctionCall(
                        None, "IPv6NumToString", (Column(None, "ip_address_v6", None),),
                    ),
                ),
            ),
            FunctionCall(
                None,
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, "event_id", None),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
        ],
    )

    TransactionColumnProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    columns = unprocessed.get_selected_columns_from_ast()
    expected = [
        "(IPv4NumToString(ip_address_v4) AS v4_ip_address)",
        "IPv6NumToString(ip_address_v6)",
        "coalesce(IPv4NumToString(ip_address_v4), IPv6NumToString(ip_address_v6))",
        "replaceAll(toString(event_id), '-', '')",
    ]
    for idx, c in enumerate(columns[1:]):
        ret = c.accept(ClickhouseExpressionFormatter())
        assert ret == expected[idx]
