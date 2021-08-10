import datetime
from typing import List, Optional

import pytest

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition, combine_and_conditions
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.parser import parse_snql_query


def build_cond(tn: str) -> str:
    time_column = "finish_ts" if tn == "t" else "timestamp"
    tn = tn + "." if tn else ""
    return f"{tn}project_id=1 AND {tn}{time_column}>=toDateTime('2021-01-01') AND {tn}{time_column}<toDateTime('2021-01-02')"


added_condition = build_cond("")


def required_condition(cond: Optional[Expression] = None) -> Expression:
    conditions: List[Expression] = [
        binary_condition(
            "equals", Column("_snuba_project_id", None, "project_id"), Literal(None, 1),
        ),
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
    ]
    if cond is not None:
        conditions.append(cond)

    return combine_and_conditions(conditions)


test_cases = [
    pytest.param(
        f"MATCH (events) SELECT 4-5, c, arrayJoin(c) AS x, tags[key] WHERE {added_condition} TOTALS true",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    name="tags[key]",
                    expression=SubscriptableReference(
                        alias="_snuba_tags[key]",
                        column=Column(
                            alias="_snuba_tags", table_name=None, column_name="tags"
                        ),
                        key=Literal(alias=None, value="key"),
                    ),
                ),
                SelectedExpression(
                    "x",
                    FunctionCall(
                        "_snuba_x", "arrayJoin", (Column("_snuba_c", None, "c"),)
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
            ],
            condition=required_condition(),
            totals=True,
            limit=1000,
            offset=0,
        ),
        id="sort select clause",
    ),
    pytest.param(
        f"MATCH (events) SELECT count() AS count BY d, 2+7, tags[key] WHERE {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "tags[key]",
                    SubscriptableReference(
                        "_snuba_tags[key]",
                        Column("_snuba_tags", None, "tags"),
                        Literal(None, "key"),
                    ),
                ),
                SelectedExpression(
                    "count", FunctionCall("_snuba_count", "count", tuple()),
                ),
                SelectedExpression("d", Column("_snuba_d", None, "d")),
                SelectedExpression(
                    "2+7",
                    FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
                ),
            ],
            groupby=[
                SubscriptableReference(
                    "_snuba_tags[key]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "key"),
                ),
                Column("_snuba_d", None, "d"),
                FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
            ],
            condition=required_condition(),
            limit=1000,
            offset=0,
        ),
        id="sort group by",
    ),
    pytest.param(
        f"MATCH (events) SELECT a WHERE time_seen<3 AND last_seen=2 AND b IN array(2, 1, 3) AND b NOT IN tuple(6, 9, 7) AND c=2 AND d=3 AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals", Column("_snuba_c", None, "c"), Literal(None, 2),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "equals", Column("_snuba_d", None, "d"), Literal(None, 3),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "equals",
                            Column("_snuba_last_seen", None, "last_seen"),
                            Literal(None, 2),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_project_id", None, "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_timestamp", None, "timestamp"),
                                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                                ),
                                binary_condition(
                                    "and",
                                    binary_condition(
                                        "in",
                                        Column("_snuba_b", None, "b"),
                                        FunctionCall(
                                            None,
                                            "array",
                                            (
                                                Literal(None, 1),
                                                Literal(None, 2),
                                                Literal(None, 3),
                                            ),
                                        ),
                                    ),
                                    binary_condition(
                                        "and",
                                        binary_condition(
                                            "less",
                                            Column(
                                                "_snuba_time_seen", None, "time_seen"
                                            ),
                                            Literal(None, 3),
                                        ),
                                        binary_condition(
                                            "and",
                                            binary_condition(
                                                "less",
                                                Column(
                                                    "_snuba_timestamp",
                                                    None,
                                                    "timestamp",
                                                ),
                                                Literal(
                                                    None,
                                                    datetime.datetime(2021, 1, 2, 0, 0),
                                                ),
                                            ),
                                            binary_condition(
                                                "notIn",
                                                Column("_snuba_b", None, "b"),
                                                FunctionCall(
                                                    None,
                                                    "tuple",
                                                    (
                                                        Literal(None, 6),
                                                        Literal(None, 7),
                                                        Literal(None, 9),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Query with multiple conditions joined by AND and arrays and tuples",
    ),
    pytest.param(
        f"MATCH (events) SELECT a WHERE ((time_seen<3 OR last_seen=afternoon) OR name=bob) AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
            condition=required_condition(
                binary_condition(
                    "or",
                    binary_condition(
                        "equals",
                        Column("_snuba_last_seen", None, "last_seen"),
                        Column("_snuba_afternoon", None, "afternoon"),
                    ),
                    binary_condition(
                        "or",
                        binary_condition(
                            "equals",
                            Column("_snuba_name", None, "name"),
                            Column("_snuba_bob", None, "bob"),
                        ),
                        binary_condition(
                            "less",
                            Column("_snuba_time_seen", None, "time_seen"),
                            Literal(None, 3),
                        ),
                    ),
                )
            ),
            limit=1000,
            offset=0,
        ),
        id="Query with multiple conditions joined by OR / parenthesized OR",
    ),
    pytest.param(
        f"MATCH (events) SELECT a WHERE (name!=bob OR last_seen<afternoon AND (location=gps(x,y,z) OR times_seen>0)) AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
            condition=required_condition(
                binary_condition(
                    "or",
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_last_seen", None, "last_seen"),
                            Column("_snuba_afternoon", None, "afternoon"),
                        ),
                        binary_condition(
                            "or",
                            binary_condition(
                                "equals",
                                Column("_snuba_location", None, "location"),
                                FunctionCall(
                                    None,
                                    "gps",
                                    (
                                        Column("_snuba_x", None, "x"),
                                        Column("_snuba_y", None, "y"),
                                        Column("_snuba_z", None, "z"),
                                    ),
                                ),
                            ),
                            binary_condition(
                                "greater",
                                Column("_snuba_times_seen", None, "times_seen"),
                                Literal(None, 0),
                            ),
                        ),
                    ),
                    binary_condition(
                        "notEquals",
                        Column("_snuba_name", None, "name"),
                        Column("_snuba_bob", None, "bob"),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Query with multiple / complex conditions joined by parenthesized / regular AND / OR",
    ),
    pytest.param(
        f"""MATCH (events)
        SELECT 4-5,3*foo(c) AS foo,c
        WHERE or(equals(arrayExists(a, '=', 'RuntimeException'), 1), equals(arrayAll(b, 'NOT IN', tuple('Stack', 'Arithmetic')), 1)) = 1 AND {added_condition}""",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("c", Column("_snuba_c", None, "c")),
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression(
                    "3*foo(c) AS foo",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
            ],
            condition=required_condition(
                binary_condition(
                    "or",
                    binary_condition(
                        "equals",
                        FunctionCall(
                            None,
                            "arrayAll",
                            (
                                Lambda(
                                    None,
                                    ("x",),
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (
                                            FunctionCall(
                                                None,
                                                "notIn",
                                                (
                                                    Argument(None, "x"),
                                                    FunctionCall(
                                                        None,
                                                        "tuple",
                                                        (
                                                            Literal(
                                                                None, "Arithmetic",
                                                            ),
                                                            Literal(None, "Stack"),
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Column("_snuba_b", None, "b"),
                            ),
                        ),
                        Literal(None, 1),
                    ),
                    binary_condition(
                        "equals",
                        FunctionCall(
                            None,
                            "arrayExists",
                            (
                                Lambda(
                                    None,
                                    ("x",),
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (
                                            FunctionCall(
                                                None,
                                                "equals",
                                                (
                                                    Argument(None, "x"),
                                                    Literal(None, "RuntimeException"),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Column("_snuba_a", None, "a"),
                            ),
                        ),
                        Literal(None, 1),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Complicated condition expressions",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: LogicalQuery) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    state.set_config("snql.sort.query.projects", "[1]")

    events = get_dataset("events")
    query = parse_snql_query(query_body, [], events)
    eq, reason = query.equals(expected_query)
    assert eq, reason
