import datetime
import pytest
from typing import Union

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinType,
)
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.parser import parse_snql_query


test_cases = [
    pytest.param(
        "MATCH (e:Events) SELECT 4-5, c",
        LogicalQuery(
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
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
                    ),
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                    ),
                ),
                keys=[],
                join_type=JoinType.INNER,
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
        """MATCH
            (e: Errors) -[contains]-> (t: Transactions),
            (e: Errors) -[assigned]-> (ga: GroupAssignee)
        SELECT 4-5, c""",
        LogicalQuery(
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
        """MATCH
            (e: Errors) -[contains]-> (t: Transactions),
            (e: Errors) -[assigned]-> (ga: GroupAssignee),
            (e: Errors) -[bookmark]-> (gm: GroupedMessage),
            (e: Errors) -[activity]-> (se: Sessions)
        SELECT 4-5, c""",
        LogicalQuery(
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
        id="Multi multi join match",
    ),
    pytest.param(
        "MATCH { MATCH (e: Events) SELECT count() AS count BY title } SELECT max(count) AS max_count",
        CompositeQuery(
            from_clause=LogicalQuery(
                {},
                QueryEntity(
                    EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
                ),
                selected_columns=[
                    SelectedExpression("count", FunctionCall(None, "count", tuple())),
                    SelectedExpression("title", Column("title", None, "title")),
                ],
            ),
            selected_columns=[
                SelectedExpression(
                    "max(count) AS max_count",
                    FunctionCall("max_count", "max", (Column("count", None, "count"),)),
                ),
            ],
        ),
        id="sub query match",
    ),
    pytest.param(
        "MATCH { MATCH { MATCH (e: Events) SELECT count() AS count BY title } SELECT max(count) AS max_count } SELECT min(count) AS min_count",
        CompositeQuery(
            from_clause=CompositeQuery(
                from_clause=LogicalQuery(
                    {},
                    QueryEntity(
                        EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
                    ),
                    selected_columns=[
                        SelectedExpression(
                            "count", FunctionCall(None, "count", tuple())
                        ),
                        SelectedExpression("title", Column("title", None, "title")),
                    ],
                ),
                selected_columns=[
                    SelectedExpression(
                        "max(count)",
                        FunctionCall(None, "max", (Column("count", None, "count"),)),
                    ),
                ],
            ),
            selected_columns=[
                SelectedExpression(
                    "min(count) AS min_count",
                    FunctionCall("min_count", "min", (Column("count", None, "count"),)),
                ),
            ],
        ),
        id="sub query of sub query match",
    ),
    pytest.param(
        "MATCH (e:Events) SELECT 4-5, c GRANULARITY 60",
        LogicalQuery(
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
            granularity=60,
        ),
        id="granularity on whole query",
    ),
    pytest.param(
        "MATCH (e:Events) SELECT 4-5, c TOTALS true",
        LogicalQuery(
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
            totals=True,
        ),
        id="totals on whole query",
    ),
    pytest.param(
        "MATCH (e:Events SAMPLE 0.5) SELECT 4-5, c",
        LogicalQuery(
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
        "MATCH (e:Events) SELECT 4-5, c LIMIT 5 BY c",
        LogicalQuery(
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
        "MATCH (e:Events) SELECT 4-5, c LIMIT 5 OFFSET 3",
        LogicalQuery(
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
        "MATCH (e:Events) SELECT 4-5, 3* foo(c) AS foo, c WHERE a<3",
        LogicalQuery(
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
                    "3* foo(c) AS foo",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall("foo", "foo", (Column("c", None, "c"),)),
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
        """MATCH (e:Events)
        SELECT 4-5,3*foo(c) AS foo,c
        WHERE a<3""",
        LogicalQuery(
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
                    "3*foo(c) AS foo",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall("foo", "foo", (Column("c", None, "c"),)),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("c", None, "c")),
            ],
            condition=binary_condition(
                None, "less", Column("a", None, "a"), Literal(None, 3)
            ),
        ),
        id="Basic query with new lines and no ambiguous clause content",
    ),
    pytest.param(
        "MATCH (e: Events) SELECT (2*(4-5)+3), foo(c) AS foo, c BY d, 2+7 WHERE a<3 ORDER BY f DESC",
        LogicalQuery(
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
                    "foo(c) AS foo",
                    FunctionCall("foo", "foo", (Column("c", None, "c"),)),
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
        LogicalQuery(
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
        "MATCH (e:Events) SELECT toDateTime('2020-01-01') AS now, 3*foo(c) AS foo BY toDateTime('2020-01-01') AS now WHERE a<3 AND timestamp>toDateTime('2020-01-01')",
        LogicalQuery(
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
                    "3*foo(c) AS foo",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall("foo", "foo", (Column("c", None, "c"),)),
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
        LogicalQuery(
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
        LogicalQuery(
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
        LogicalQuery(
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
def test_format_expressions(
    query_body: str, expected_query: Union[LogicalQuery, CompositeQuery[QueryEntity]]
) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")
    query = parse_snql_query(query_body, events)

    # TODO: Add this back once query equality is added (#1457)
    # assert query.get_from_clause() == expected_query.get_from_clause()
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
