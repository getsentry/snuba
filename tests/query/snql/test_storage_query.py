import datetime

import pytest

from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.simple import Storage as QueryStorage
from snuba.query.dsl import NestedColumn, and_cond, equals
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, StorageQuery
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import parse_snql_query

tags = NestedColumn("tags")


class DummyEntity(QueryEntity):
    def __eq__(self, other):
        return True


def build_cond(tn: str) -> str:
    time_column = "end_timestamp"
    tn = tn + "." if tn else ""
    return f"{tn}project_id=1 AND {tn}{time_column}>=toDateTime('2021-01-01') AND {tn}{time_column}<toDateTime('2021-01-02')"


added_condition = build_cond("")

required_conditions = [
    equals(
        Column("_snuba_project_id", None, "project_id"),
        Literal(None, 1),
    ),
    binary_condition(
        "greaterOrEquals",
        Column("_snuba_end_timestamp", None, "end_timestamp"),
        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
    ),
    binary_condition(
        "less",
        Column("_snuba_end_timestamp", None, "end_timestamp"),
        Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
    ),
]
required_condition = and_cond(
    required_conditions[0],
    and_cond(
        required_conditions[1],
        required_conditions[2],
    ),
)


test_cases = [
    pytest.param(
        f"MATCH STORAGE(eap_spans) SELECT 4-5, trace_id WHERE {added_condition} GRANULARITY 60",
        StorageQuery.from_query(
            Query(
                QueryStorage(key=StorageKey("eap_spans")),
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
            )
        ),
        id="basic_storage_query",
    ),
    pytest.param(
        f"MATCH STORAGE(eap_spans) SELECT trace_id WHERE tags[something] = 'something_else' AND {added_condition} ",
        StorageQuery.from_query(
            Query(
                QueryStorage(key=StorageKey("eap_spans")),
                selected_columns=[
                    SelectedExpression(
                        "trace_id", Column("_snuba_trace_id", None, "trace_id")
                    ),
                ],
                granularity=None,
                condition=and_cond(
                    and_cond(
                        equals(tags["something"], Literal(None, "something_else")),
                        required_conditions[0],
                    ),
                    and_cond(required_conditions[1], required_conditions[2]),
                ),
                limit=1000,
                offset=0,
            )
        ),
        id="nested field query",
    ),
    pytest.param(
        f"MATCH STORAGE(eap_spans SAMPLE 0.1) SELECT trace_id WHERE tags[something] = 'something_else' AND {added_condition} ",
        StorageQuery.from_query(
            Query(
                QueryStorage(key=StorageKey("eap_spans"), sample=0.1),
                selected_columns=[
                    SelectedExpression(
                        "trace_id", Column("_snuba_trace_id", None, "trace_id")
                    ),
                ],
                granularity=None,
                condition=and_cond(
                    and_cond(
                        equals(tags["something"], "something_else"),
                        required_conditions[0],
                    ),
                    and_cond(required_conditions[1], required_conditions[2]),
                ),
                limit=1000,
                offset=0,
            )
        ),
        id="basic_query-sample",
    ),
    pytest.param(
        """MATCH {
            MATCH STORAGE(eap_spans) SELECT trace_id, duration_ms WHERE %s LIMIT 100
        } SELECT max(duration_ms) AS max_duration LIMIT 100"""
        % added_condition,
        CompositeQuery(
            selected_columns=[
                SelectedExpression(
                    "max_duration",
                    FunctionCall(
                        "_snuba_max_duration",
                        "max",
                        (Column("_snuba_duration_ms", None, "_snuba_duration_ms"),),
                    ),
                )
            ],
            from_clause=Query(
                QueryStorage(key=StorageKey("eap_spans")),
                selected_columns=[
                    SelectedExpression(
                        "trace_id", Column("_snuba_trace_id", None, "trace_id")
                    ),
                    SelectedExpression(
                        "duration_ms", Column("_snuba_duration_ms", None, "duration_ms")
                    ),
                ],
                granularity=None,
                condition=required_condition,
                limit=100,
                offset=0,
            ),
            limit=100,
        ),
        id="composite_query",
    ),
    pytest.param(
        """ MATCH STORAGE(eap_spans) SELECT trace_id, duration_ms AS duration WHERE %s LIMIT 100"""
        % added_condition,
        StorageQuery.from_query(
            Query(
                QueryStorage(key=StorageKey("eap_spans")),
                selected_columns=[
                    SelectedExpression(
                        "trace_id", Column("_snuba_trace_id", None, "trace_id")
                    ),
                    SelectedExpression(
                        "duration", Column("_snuba_duration_ms", None, "duration_ms")
                    ),
                ],
                granularity=None,
                condition=required_condition,
                limit=100,
                offset=0,
            )
        ),
        id="subquery",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_parse_storage_query(query_body: str, expected_query: StorageQuery) -> None:
    # dataset does not matter :D
    events = get_dataset("events")
    query = parse_snql_query(query_body, events)
    eq, reason = query.equals(expected_query)
    # this is an easier diff to parse as a human
    assert repr(query) == repr(expected_query)
    # get a structural diff too
    assert eq, reason


def test_fail_join() -> None:
    # dataset does not matter :D
    query_body = """ MATCH STORAGE(eap_spans: m) -[something]-> (t: transactions) SELECT trace_id, duration_ms AS duration WHERE %s LIMIT 100"""
    events = get_dataset("events")
    with pytest.raises(ParsingException):
        parse_snql_query(query_body, events)
