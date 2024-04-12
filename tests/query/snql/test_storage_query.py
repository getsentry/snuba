import datetime

import pytest

from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.dsl import NestedColumn, and_cond, equals
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.parser import parse_snql_query

tags = NestedColumn("tags")


def build_cond(tn: str) -> str:
    time_column = "finish_ts" if tn == "t" else "timestamp"
    tn = tn + "." if tn else ""
    return f"{tn}project_id=1 AND {tn}{time_column}>=toDateTime('2021-01-01') AND {tn}{time_column}<toDateTime('2021-01-02')"


added_condition = build_cond("")

required_condition = and_cond(
    equals(
        Column("_snuba_project_id", None, "project_id"),
        Literal(None, 1),
    ),
    and_cond(
        binary_condition(
            "greaterOrEquals",
            Column("_snuba_timestamp", None, "timestamp"),
            Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
        ),
        binary_condition(
            "less",
            Column("_snuba_timestamp", None, "timestamp"),
            Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
        ),
    ),
)


test_cases = [
    pytest.param(
        f"MATCH STORAGE(metric_summaries) SELECT 4-5, trace_id WHERE {added_condition} GRANULARITY 60",
        LogicalQuery(
            QueryStorage(storage_key=StorageKey("metric_summaries")),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression(
                    "trace_id", Column("_snuba_trace_id", None, "trace_id")
                ),
            ],
            granularity=60,
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="basic_storage_query",
    ),
    pytest.param(
        f"MATCH STORAGE(metrics_summaries) SELECT trace_id WHERE tags[something] = 'something_else' AND {added_condition} ",
        LogicalQuery(
            QueryStorage(storage_key=StorageKey("metrics_summaries")),
            selected_columns=[
                SelectedExpression(
                    "trace_id", Column("_snuba_trace_id", None, "trace_id")
                ),
            ],
            granularity=None,
            condition=and_cond(
                equals(tags["something"], Literal(None, "something_else")),
                required_condition,
            ),
            limit=1000,
            offset=0,
        ),
        id="basic_storage_query",
    ),
    # test sample
    # test composite query
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: LogicalQuery) -> None:
    # dataset does not matter :D
    events = get_dataset("events")
    query, _ = parse_snql_query(query_body, events)
    eq, reason = query.equals(expected_query)
    print(query)
    print("------")
    print(expected_query)
    assert repr(query) == repr(expected_query)
    assert eq, reason
