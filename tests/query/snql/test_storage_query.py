import datetime

import pytest

from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.dsl import NestedColumn, and_cond, equals
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import StorageQuery
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
        StorageQuery(
            QueryStorage(key=StorageKey("metric_summaries")),
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
        StorageQuery(
            QueryStorage(key=StorageKey("metrics_summaries")),
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
        id="nested field query",
    ),
    pytest.param(
        f"MATCH STORAGE(metrics_summaries SAMPLE 0.1) SELECT trace_id WHERE tags[something] = 'something_else' AND {added_condition} ",
        StorageQuery(
            QueryStorage(key=StorageKey("metrics_summaries"), sample=0.1),
            selected_columns=[
                SelectedExpression(
                    "trace_id", Column("_snuba_trace_id", None, "trace_id")
                ),
            ],
            granularity=None,
            condition=and_cond(
                equals(tags["something"], "something_else"),
                required_condition,
            ),
            limit=1000,
            offset=0,
        ),
        id="basic_query-sample",
    ),
    pytest.param(
        """MATCH {
            MATCH STORAGE(metrics_summaries) SELECT trace_id, duration_ms AS duration WHERE %s LIMIT 100
        } SELECT max(duration_ms) AS max_duration"""
        % added_condition,
        CompositeQuery(
            from_clause=StorageQuery(
                QueryStorage(key=StorageKey("metrics_summaries"), sample=0.1),
                selected_columns=[
                    SelectedExpression(
                        "trace_id", Column("_snuba_trace_id", None, "trace_id")
                    ),
                    SelectedExpression(
                        "duration", Column("_snuba_duration", None, "duration_ms")
                    ),
                ],
                granularity=None,
                condition=required_condition,
                limit=100,
                offset=0,
            ),
            selected_columns=[
                SelectedExpression(
                    "max_duration",
                    FunctionCall(
                        "_snuba_max_duration",
                        "max",
                        (Column("_snuba_duration", None, "_snuba_duration"),),
                    ),
                )
            ],
        ),
        id="composite_query",
    ),
    # test groupby
    # test join doesn't work
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: StorageQuery) -> None:
    # dataset does not matter :D
    events = get_dataset("events")
    query, _ = parse_snql_query(query_body, events)
    eq, reason = query.equals(expected_query)
    # this is an easier diff to parse as a human
    assert repr(query) == repr(expected_query)
    # get a structural diff too
    assert eq, reason
