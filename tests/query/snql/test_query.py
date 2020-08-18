import datetime
import pytest

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.logical import Query
from snuba.query.snql.parser import parse_snql_query


test_cases = [
    pytest.param(
        "MATCH(e:Events) SELECT 4-5, c",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
        ),
        id="Basic entity match",
    ),
    pytest.param(
        "MATCH (e: Events) -[contains]-> (t: Transactions) SELECT 4-5, c",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
        ),
        id="Basic join match",
    ),
    pytest.param(
        "MATCH (e: Errors) -[contains]-> (t: Transactions), (e: Errors) -[assigned]-> (ga: GroupAssignee) SELECT 4-5, c",
        Query(
            {},
            QueryEntity(
                EntityKey.ERRORS, get_entity(EntityKey.ERRORS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
        ),
        id="Multi join match",
    ),
    pytest.param(
        "MATCH { MATCH (e: Events) SELECT count() BY title } SELECT max(count)",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "max(count)",
                    FunctionCall(None, "max", (Column("count", None, "count"),)),
                ),
            ],
        ),
        id="sub query match",
    ),
    pytest.param(
        "MATCH { MATCH { MATCH (e: Events) SELECT count() BY title } SELECT max(count) } SELECT min(count)",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "min(count)",
                    FunctionCall(None, "min", (Column("count", None, "count"),)),
                ),
            ],
        ),
        id="sub query of sub query match",
    ),
    pytest.param(
        "MATCH(e:Events) SELECT 4-5, c SAMPLE 0.5",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
            sample=0.5,
        ),
        id="sample on whole query",
    ),
    pytest.param(
        "MATCH(e:Events SAMPLE 0.5) SELECT 4-5, c",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), 0.5
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
            sample=0.5,
        ),
        id="sample on entity",
    ),
    pytest.param(
        "MATCH(e:Events) SELECT 4-5, c LIMIT 5 BY c",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
            limitby=(1, "c"),
        ),
        id="limit by column",
    ),
    pytest.param(
        "MATCH(e:Events) SELECT 4-5, c LIMIT 5 OFFSET 3",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
            limit=5,
            offset=3,
        ),
        id="limit and offset",
    ),
    pytest.param(
        "MATCH(e:Events)SELECT4-5,3*foo(c),c WHEREa<3",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression(
                    "3*foo(c)",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(None, "foo", (Column("c", None, "c"),)),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
            condition=binary_condition(
                None, "less", Column("a", None, "a"), Literal(None, 3)
            ),
        ),
        id="Basic query with no spaces and no ambiguous clause content",
    ),
    pytest.param(
        "MATCH (e: Events) SELECT (2*(4-5)+3), foo(c), c BY d, 2+7 WHERE a<3 ORDER BY f DESC",
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
                    "foo(c)", FunctionCall(None, "foo", (Column("c", None, "c"),)),
                ),
                SelectedExpression("c", Column("c", None, "c")),
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
        id="Simple complete query with example of parenthesized arithmetic expression in SELECT",
    ),
    pytest.param(
        "MATCH (e: Events) SELECT (2*(4-5)+3), foo(c) AS thing2, c BY d, 2+7 WHERE a<3 ORDER BY f DESC",
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
                    "foo(c) AS thing2",
                    FunctionCall("thing2", "foo", (Column("c", None, "c"),)),
                ),
                SelectedExpression("c", Column("c", None, "c")),
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
        id="Simple complete query with aliased function in SELECT",
    ),
    pytest.param(
        "MATCH (e:Events) SELECT toDateTime('2020-01-01') AS now, 3*foo(c) BY toDateTime('2020-01-01') AS now WHERE a<3 AND timestamp>toDateTime('2020-01-01')",
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "toDateTime('2020-01-01') AS now",
                    Literal("now", datetime.datetime(2020, 1, 1, 0, 0)),
                ),
                SelectedExpression(
                    "3*foo(c)",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(None, "foo", (Column("c", None, "c"),)),
                        ),
                    ),
                ),
            ],
            groupby=[Literal("now", datetime.datetime(2020, 1, 1, 0, 0))],
            condition=binary_condition(
                None,
                "and",
                binary_condition(
                    None, "less", Column("a", None, "a"), Literal(None, 3)
                ),
                binary_condition(
                    None,
                    "greater",
                    Column("timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2020, 1, 1, 0, 0)),
                ),
            ),
        ),
        id="Basic query with date literals",
    ),
    pytest.param(
        "MATCH (e: Events) SELECT a WHERE time_seen<3 AND last_seen=2 AND c=2 AND d=3",
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
        "MATCH (e: Events) SELECT a WHERE (time_seen<3 OR last_seen=afternoon) OR name=bob",
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
        "MATCH (e: Events) SELECT a WHERE name!=bob OR last_seen<afternoon AND (location=gps(x,y,z) OR times_seen>0)",
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
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: Query) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events").get_default_entity()
    query = parse_snql_query(query_body, events)

    assert query.get_from_clause() == expected_query.get_from_clause()
    assert (
        query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()
    assert query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert query.get_having_from_ast() == expected_query.get_having_from_ast()
    assert query.get_limit() == expected_query.get_limit()
    assert query.get_offset() == expected_query.get_offset()
    assert query.get_sample() == expected_query.get_sample()
