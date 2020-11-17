import pytest

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.logical import Query
from snuba.query.snql.parser import parse_snql_query


test_cases = [
    pytest.param(
        "MATCH(e: events)WHEREa<3COLLECT4-5,3*g(c),c",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5),),),
                ),
                SelectedExpression(
                    "3*g(c)",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(None, "g", (Column("c", None, "c"),),),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("c", None, "c"),),
            ],
            condition=binary_condition(
                None, "less", Column("a", None, "a"), Literal(None, 3)
            ),
        ),
        id="Basic query with no spaces and no ambiguous clause content",
    ),
    pytest.param(
        "MATCH (e: events) WHERE a<3 COLLECT (2*(4-5)+3), g(c), c BY d, 2+7 ORDER BY f DESC",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "(2*(4-5)+3)",
                    FunctionCall(
                        None,
                        "plus",
                        (
                            FunctionCall(
                                None,
                                "multiply",
                                (
                                    Literal(None, 2),
                                    FunctionCall(
                                        None,
                                        "minus",
                                        (Literal(None, 4), Literal(None, 5)),
                                    ),
                                ),
                            ),
                            Literal(None, 3),
                        ),
                    ),
                ),
                SelectedExpression(
                    "g(c)", FunctionCall(None, "g", (Column("c", None, "c"),)),
                ),
                SelectedExpression("c", Column("c", None, "c")),
                SelectedExpression("d", Column("d", None, "d")),
                SelectedExpression(
                    "2+7",
                    FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
                ),
            ],
            condition=binary_condition(
                None, "less", Column("a", None, "a"), Literal(None, 3)
            ),
            groupby=[
                Column("d", None, "d"),
                FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
            ],
            order_by=[OrderBy(OrderByDirection.DESC, Column("f", None, "f"))],
        ),
        id="Simple complete query with example of parenthesized arithmetic expression in COLLECT",
    ),
    pytest.param(
        "MATCH (e: events) WHERE time_seen<3 AND last_seen=2 AND c=2 AND d=3 COLLECT a",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("a", None, "a"))],
            condition=FunctionCall(
                None,
                "and",
                (
                    FunctionCall(
                        None,
                        "less",
                        (Column("time_seen", None, "time_seen"), Literal(None, 3)),
                    ),
                    FunctionCall(
                        None,
                        "and",
                        (
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column("last_seen", None, "last_seen"),
                                    Literal(None, 2),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "and",
                                (
                                    FunctionCall(
                                        None,
                                        "equals",
                                        (Column("c", None, "c"), Literal(None, 2)),
                                    ),
                                    FunctionCall(
                                        None,
                                        "equals",
                                        (Column("d", None, "d"), Literal(None, 3)),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        id="Query with multiple conditions joined by AND",
    ),
    pytest.param(
        "MATCH (e: events) WHERE (time_seen<3 OR last_seen=afternoon) OR name=bob COLLECT a",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("a", None, "a"))],
            condition=FunctionCall(
                None,
                "or",
                (
                    FunctionCall(
                        None,
                        "or",
                        (
                            FunctionCall(
                                None,
                                "less",
                                (
                                    Column("time_seen", None, "time_seen"),
                                    Literal(None, 3),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column("last_seen", None, "last_seen"),
                                    Column("afternoon", None, "afternoon"),
                                ),
                            ),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "equals",
                        (Column("name", None, "name"), Column("bob", None, "bob")),
                    ),
                ),
            ),
        ),
        id="Query with multiple conditions joined by OR / parenthesized OR",
    ),
    pytest.param(
        "MATCH (e: events) WHERE name!=bob OR last_seen<afternoon AND (location=gps(x,y,z) OR times_seen>0) COLLECT a",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("a", None, "a"),)],
            condition=FunctionCall(
                None,
                "or",
                (
                    FunctionCall(
                        None,
                        "notEquals",
                        (Column("name", None, "name"), Column("bob", None, "bob")),
                    ),
                    FunctionCall(
                        None,
                        "and",
                        (
                            FunctionCall(
                                None,
                                "less",
                                (
                                    Column("last_seen", None, "last_seen"),
                                    Column("afternoon", None, "afternoon"),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "or",
                                (
                                    FunctionCall(
                                        None,
                                        "equals",
                                        (
                                            Column("location", None, "location"),
                                            FunctionCall(
                                                None,
                                                "gps",
                                                (
                                                    Column("x", None, "x"),
                                                    Column("y", None, "y"),
                                                    Column("z", None, "z"),
                                                ),
                                            ),
                                        ),
                                    ),
                                    FunctionCall(
                                        None,
                                        "greater",
                                        (
                                            Column("times_seen", None, "times_seen"),
                                            Literal(None, 0),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        id="Query with multiple / complex conditions joined by parenthesized / regular AND / OR",
    ),
    pytest.param(
        "MATCH (e: events) WHERE project_id IN tuple( 2 , 3) COLLECT a, b[c]",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("a", Column("a", None, "a")),
                SelectedExpression(
                    "b[c]",
                    SubscriptableReference(
                        "b[c]", Column("b", None, "b"), key=Literal(None, "c")
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "in",
                (
                    Column("project_id", None, "project_id",),
                    FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 3))),
                ),
            ),
        ),
        id="Query with IN condition",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: Query) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")
    query = parse_snql_query(query_body, events)

    eq, reason = query.equals(expected_query)
    assert eq, reason
