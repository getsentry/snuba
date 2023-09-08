import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition, unary_condition
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinRelationship,
    JoinType,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Argument,
    Column,
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
required_condition = binary_condition(
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
            "less",
            Column("_snuba_timestamp", None, "timestamp"),
            Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
        ),
    ),
)


test_cases = [
    pytest.param(
        f"MATCH (events) SELECT 4-5, c WHERE {added_condition} GRANULARITY 60",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            granularity=60,
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="granularity on whole query",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, c WHERE {added_condition} TOTALS true",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            totals=True,
            limit=1000,
            offset=0,
        ),
        id="totals on whole query",
    ),
    pytest.param(
        f"MATCH (events SAMPLE 0.5) SELECT 4-5, c WHERE {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS,
                get_entity(EntityKey.EVENTS).get_data_model(),
                0.5,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            sample=0.5,
            limit=1000,
            offset=0,
        ),
        id="sample on entity",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, c,d,e WHERE {added_condition} LIMIT 5 BY c,d,e",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
                SelectedExpression("d", Column("_snuba_d", None, "d")),
                SelectedExpression("e", Column("_snuba_e", None, "e")),
            ],
            condition=required_condition,
            limitby=LimitBy(
                5,
                [
                    Column("_snuba_c", None, "c"),
                    Column("_snuba_d", None, "d"),
                    Column("_snuba_e", None, "e"),
                ],
            ),
            limit=1000,
            offset=0,
        ),
        id="limit by multiple columns",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, c WHERE {added_condition} LIMIT 5 BY c",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            limitby=LimitBy(5, [Column("_snuba_c", None, "c")]),
            limit=1000,
            offset=0,
        ),
        id="limit by single column",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, c WHERE {added_condition} LIMIT 5 OFFSET 3",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            limit=5,
            offset=3,
        ),
        id="limit and offset",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, c, arrayJoin(c) AS x WHERE {added_condition} TOTALS true",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
                SelectedExpression(
                    "x",
                    FunctionCall(
                        "_snuba_x", "arrayJoin", (Column("_snuba_c", None, "c"),)
                    ),
                ),
            ],
            condition=required_condition,
            totals=True,
            limit=1000,
            offset=0,
        ),
        id="Array join",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, 3* foo(c) AS foo, c WHERE {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression(
                    "3* foo(c) AS foo",
                    FunctionCall(
                        "_snuba_3* foo(c) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="Basic query with no spaces and no ambiguous clause content",
    ),
    pytest.param(
        f"""MATCH (events)
        SELECT 4-5,3*foo(c) AS foo,c
        WHERE platform NOT IN tuple('x', 'y') AND message IS NULL
        AND {added_condition}""",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression(
                    "3*foo(c) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(c) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "notIn",
                    Column("_snuba_platform", None, "platform"),
                    FunctionCall(
                        None, "tuple", (Literal(None, "x"), Literal(None, "y"))
                    ),
                ),
                binary_condition(
                    "and",
                    unary_condition(
                        "isNull", Column("_snuba_message", None, "message")
                    ),
                    required_condition,
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Basic query with word condition ops",
    ),
    pytest.param(
        f"MATCH (events) SELECT count() AS count BY tags[key], measurements[lcp.elementSize] WHERE measurements[lcp.elementSize] > 1 AND {added_condition}",
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
                    "measurements[lcp.elementSize]",
                    SubscriptableReference(
                        "_snuba_measurements[lcp.elementSize]",
                        Column("_snuba_measurements", None, "measurements"),
                        Literal(None, "lcp.elementSize"),
                    ),
                ),
                SelectedExpression(
                    "count",
                    FunctionCall("_snuba_count", "count", tuple()),
                ),
            ],
            groupby=[
                SubscriptableReference(
                    "_snuba_tags[key]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "key"),
                ),
                SubscriptableReference(
                    "_snuba_measurements[lcp.elementSize]",
                    Column("_snuba_measurements", None, "measurements"),
                    Literal(None, "lcp.elementSize"),
                ),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "greater",
                    SubscriptableReference(
                        "_snuba_measurements[lcp.elementSize]",
                        Column("_snuba_measurements", None, "measurements"),
                        Literal(None, "lcp.elementSize"),
                    ),
                    Literal(None, 1),
                ),
                required_condition,
            ),
            limit=1000,
            offset=0,
        ),
        id="Basic query with subscriptables",
    ),
    pytest.param(
        f"MATCH (events) SELECT (2*(4-5)+3), g(c) AS goo, c BY d, 2+7 WHERE {added_condition} ORDER BY f DESC",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("d", Column("_snuba_d", None, "d")),
                SelectedExpression(
                    "2+7",
                    FunctionCall(
                        "_snuba_2+7", "plus", (Literal(None, 2), Literal(None, 7))
                    ),
                ),
                SelectedExpression(
                    "(2*(4-5)+3)",
                    FunctionCall(
                        "_snuba_(2*(4-5)+3)",
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
                    "goo",
                    FunctionCall("_snuba_goo", "g", (Column("_snuba_c", None, "c"),)),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            groupby=[
                Column("_snuba_d", None, "d"),
                FunctionCall(
                    "_snuba_2+7", "plus", (Literal(None, 2), Literal(None, 7))
                ),
            ],
            order_by=[OrderBy(OrderByDirection.DESC, Column("_snuba_f", None, "f"))],
            limit=1000,
            offset=0,
        ),
        id="Simple complete query with example of parenthesized arithmetic expression in SELECT",
    ),
    pytest.param(
        f"MATCH (events) SELECT (2*(4-5)+3), foo(c) AS thing2, c BY d, 2+7 WHERE {added_condition} ORDER BY f DESC",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("d", Column("_snuba_d", None, "d")),
                SelectedExpression(
                    "2+7",
                    FunctionCall(
                        "_snuba_2+7", "plus", (Literal(None, 2), Literal(None, 7))
                    ),
                ),
                SelectedExpression(
                    "(2*(4-5)+3)",
                    FunctionCall(
                        "_snuba_(2*(4-5)+3)",
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
                    "thing2",
                    FunctionCall(
                        "_snuba_thing2", "foo", (Column("_snuba_c", None, "c"),)
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            groupby=[
                Column("_snuba_d", None, "d"),
                FunctionCall(
                    "_snuba_2+7", "plus", (Literal(None, 2), Literal(None, 7))
                ),
            ],
            order_by=[OrderBy(OrderByDirection.DESC, Column("_snuba_f", None, "f"))],
            limit=1000,
            offset=0,
        ),
        id="Simple complete query with aliased function in SELECT",
    ),
    pytest.param(
        f"MATCH (events) SELECT toDateTime('2020-01-01') AS now, 3*foo(c) AS foo BY toDateTime('2020-01-01') AS now WHERE {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "now",
                    Literal("_snuba_now", datetime.datetime(2020, 1, 1, 0, 0)),
                ),
                SelectedExpression(
                    "now",
                    Literal("_snuba_now", datetime.datetime(2020, 1, 1, 0, 0)),
                ),
                SelectedExpression(
                    "3*foo(c) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(c) AS foo",
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
            groupby=[Literal("_snuba_now", datetime.datetime(2020, 1, 1, 0, 0))],
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="Basic query with date literals",
    ),
    pytest.param(
        f"MATCH (events) SELECT a WHERE time_seen<3 AND last_seen=2 AND c=2 AND d=3 AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
            condition=binary_condition(
                "and",
                binary_condition(
                    "less",
                    Column("_snuba_time_seen", None, "time_seen"),
                    Literal(None, 3),
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
                            Column("_snuba_c", None, "c"),
                            Literal(None, 2),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_d", None, "d"),
                                Literal(None, 3),
                            ),
                            required_condition,
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Query with multiple conditions joined by AND",
    ),
    pytest.param(
        f"MATCH (events) SELECT a WHERE ((time_seen<3 OR last_seen=afternoon) OR name=bob) AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
            condition=binary_condition(
                "and",
                binary_condition(
                    "or",
                    binary_condition(
                        "or",
                        binary_condition(
                            "less",
                            Column("_snuba_time_seen", None, "time_seen"),
                            Literal(None, 3),
                        ),
                        binary_condition(
                            "equals",
                            Column("_snuba_last_seen", None, "last_seen"),
                            Column("_snuba_afternoon", None, "afternoon"),
                        ),
                    ),
                    binary_condition(
                        "equals",
                        Column("_snuba_name", None, "name"),
                        Column("_snuba_bob", None, "bob"),
                    ),
                ),
                required_condition,
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
            condition=binary_condition(
                "and",
                binary_condition(
                    "or",
                    binary_condition(
                        "notEquals",
                        Column("_snuba_name", None, "name"),
                        Column("_snuba_bob", None, "bob"),
                    ),
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
                ),
                required_condition,
            ),
            limit=1000,
            offset=0,
        ),
        id="Query with multiple / complex conditions joined by parenthesized / regular AND / OR",
    ),
    pytest.param(
        """MATCH (events)
        SELECT a, b[c]
        WHERE project_id IN tuple( 2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')""",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("a", Column("_snuba_a", None, "a")),
                SelectedExpression(
                    "b[c]",
                    SubscriptableReference(
                        "_snuba_b[c]",
                        Column("_snuba_b", None, "b"),
                        key=Literal(None, "c"),
                    ),
                ),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "in",
                    Column("_snuba_project_id", None, "project_id"),
                    FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 3))),
                ),
                binary_condition(
                    "and",
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
            ),
            limit=1000,
            offset=0,
        ),
        id="Query with IN condition",
    ),
    pytest.param(
        f"""MATCH (events)
        SELECT 4-5,3*foo(c) AS foo,c
        WHERE {added_condition}""",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression(
                    "3*foo(c) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(c) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="Basic query with new lines and no ambiguous clause content",
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
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression(
                    "3*foo(c) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(c) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    binary_condition(
                        "or",
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
                                                        Literal(
                                                            None, "RuntimeException"
                                                        ),
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
                                                                Literal(None, "Stack"),
                                                                Literal(
                                                                    None,
                                                                    "Arithmetic",
                                                                ),
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
                    ),
                    Literal(None, 1),
                ),
                required_condition,
            ),
            limit=1000,
            offset=0,
        ),
        id="Special array join functions",
    ),
    pytest.param(
        f"""MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c
        WHERE {build_cond('e')} AND {build_cond('t')}""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS,
                        get_entity(EntityKey.EVENTS).get_data_model(),
                    ),
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("e.c", Column("_snuba_e.c", "e", "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_e.project_id", "e", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "greaterOrEquals",
                        Column("_snuba_e.timestamp", "e", "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_e.timestamp", "e", "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_t.project_id", "t", "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                                ),
                                binary_condition(
                                    "less",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Basic join match",
    ),
    pytest.param(
        f"""MATCH (e: events) -[contains]-> (t: transactions SAMPLE 0.5) SELECT 4-5, t.c
        WHERE {build_cond('e')} AND {build_cond('t')}""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS,
                        get_entity(EntityKey.EVENTS).get_data_model(),
                    ),
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                        0.5,
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("t.c", Column("_snuba_t.c", "t", "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_e.project_id", "e", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "greaterOrEquals",
                        Column("_snuba_e.timestamp", "e", "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_e.timestamp", "e", "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_t.project_id", "t", "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                                ),
                                binary_condition(
                                    "less",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Basic join match with sample",
    ),
    pytest.param(
        f"""MATCH
            (e: events) -[contains]-> (t: transactions),
            (e: events) -[assigned]-> (ga: groupassignee)
        SELECT 4-5, ga.c
        WHERE {build_cond('e')} AND {build_cond('t')}""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=JoinClause(
                    left_node=IndividualNode(
                        "e",
                        QueryEntity(
                            EntityKey.EVENTS,
                            get_entity(EntityKey.EVENTS).get_data_model(),
                        ),
                    ),
                    right_node=IndividualNode(
                        "ga",
                        QueryEntity(
                            EntityKey.GROUPASSIGNEE,
                            get_entity(EntityKey.GROUPASSIGNEE).get_data_model(),
                        ),
                    ),
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("e", "event_id"),
                            JoinConditionExpression("ga", "group_id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("ga.c", Column("_snuba_ga.c", "ga", "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_e.project_id", "e", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "greaterOrEquals",
                        Column("_snuba_e.timestamp", "e", "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_e.timestamp", "e", "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_t.project_id", "t", "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                                ),
                                binary_condition(
                                    "less",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Multi join match",
    ),
    pytest.param(
        f"""MATCH
            (e: events) -[contains]-> (t: transactions),
            (e: events) -[assigned]-> (ga: groupassignee),
            (e: events) -[bookmark]-> (gm: groupedmessage),
            (e: events) -[activity]-> (se: sessions)
        SELECT 4-5, e.a, t.b, ga.c, gm.d, se.e
        WHERE {build_cond('e')} AND {build_cond('t')}
        AND se.org_id = 1 AND se.project_id = 1
        AND se.started >= toDateTime('2021-01-01') AND se.started < toDateTime('2021-01-02')""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=JoinClause(
                    left_node=JoinClause(
                        left_node=JoinClause(
                            left_node=IndividualNode(
                                "e",
                                QueryEntity(
                                    EntityKey.EVENTS,
                                    get_entity(EntityKey.EVENTS).get_data_model(),
                                ),
                            ),
                            right_node=IndividualNode(
                                "se",
                                QueryEntity(
                                    EntityKey.SESSIONS,
                                    get_entity(EntityKey.SESSIONS).get_data_model(),
                                ),
                            ),
                            keys=[
                                JoinCondition(
                                    JoinConditionExpression("e", "event_id"),
                                    JoinConditionExpression("se", "org_id"),
                                )
                            ],
                            join_type=JoinType.INNER,
                        ),
                        right_node=IndividualNode(
                            "gm",
                            QueryEntity(
                                EntityKey.GROUPEDMESSAGE,
                                get_entity(EntityKey.GROUPEDMESSAGE).get_data_model(),
                            ),
                        ),
                        keys=[
                            JoinCondition(
                                JoinConditionExpression("e", "event_id"),
                                JoinConditionExpression("gm", "first_release_id"),
                            )
                        ],
                        join_type=JoinType.INNER,
                    ),
                    right_node=IndividualNode(
                        "ga",
                        QueryEntity(
                            EntityKey.GROUPASSIGNEE,
                            get_entity(EntityKey.GROUPASSIGNEE).get_data_model(),
                        ),
                    ),
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("e", "event_id"),
                            JoinConditionExpression("ga", "group_id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression("e.a", Column("_snuba_e.a", "e", "a")),
                SelectedExpression("t.b", Column("_snuba_t.b", "t", "b")),
                SelectedExpression("ga.c", Column("_snuba_ga.c", "ga", "c")),
                SelectedExpression("gm.d", Column("_snuba_gm.d", "gm", "d")),
                SelectedExpression("se.e", Column("_snuba_se.e", "se", "e")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_e.project_id", "e", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "greaterOrEquals",
                        Column("_snuba_e.timestamp", "e", "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_e.timestamp", "e", "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_t.project_id", "t", "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                                ),
                                binary_condition(
                                    "and",
                                    binary_condition(
                                        "less",
                                        Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                        Literal(
                                            None, datetime.datetime(2021, 1, 2, 0, 0)
                                        ),
                                    ),
                                    binary_condition(
                                        "and",
                                        binary_condition(
                                            "equals",
                                            Column("_snuba_se.org_id", "se", "org_id"),
                                            Literal(None, 1),
                                        ),
                                        binary_condition(
                                            "and",
                                            binary_condition(
                                                "equals",
                                                Column(
                                                    "_snuba_se.project_id",
                                                    "se",
                                                    "project_id",
                                                ),
                                                Literal(None, 1),
                                            ),
                                            binary_condition(
                                                "and",
                                                binary_condition(
                                                    "greaterOrEquals",
                                                    Column(
                                                        "_snuba_se.started",
                                                        "se",
                                                        "started",
                                                    ),
                                                    Literal(
                                                        None,
                                                        datetime.datetime(
                                                            2021, 1, 1, 0, 0
                                                        ),
                                                    ),
                                                ),
                                                binary_condition(
                                                    "less",
                                                    Column(
                                                        "_snuba_se.started",
                                                        "se",
                                                        "started",
                                                    ),
                                                    Literal(
                                                        None,
                                                        datetime.datetime(
                                                            2021, 1, 2, 0, 0
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
            ),
            limit=1000,
            offset=0,
        ),
        id="Multi multi join match",
    ),
    pytest.param(
        "MATCH { MATCH (events) SELECT count() AS count BY title WHERE %s } SELECT max(count) AS max_count"
        % added_condition,
        CompositeQuery(
            from_clause=LogicalQuery(
                QueryEntity(
                    EntityKey.EVENTS,
                    get_entity(EntityKey.EVENTS).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression("title", Column("_snuba_title", None, "title")),
                    SelectedExpression(
                        "count", FunctionCall("_snuba_count", "count", tuple())
                    ),
                ],
                groupby=[Column("_snuba_title", None, "title")],
                condition=required_condition,
            ),
            selected_columns=[
                SelectedExpression(
                    "max_count",
                    FunctionCall(
                        "_snuba_max_count",
                        "max",
                        (Column("_snuba_count", None, "_snuba_count"),),
                    ),
                ),
            ],
            limit=1000,
            offset=0,
        ),
        id="sub query match",
    ),
    pytest.param(
        """MATCH {
            MATCH {
                MATCH (events) SELECT count() AS count BY title WHERE %s
            }
            SELECT max(count) AS max_count
        }
        SELECT min(max_count) AS min_count"""
        % added_condition,
        CompositeQuery(
            from_clause=CompositeQuery(
                from_clause=LogicalQuery(
                    QueryEntity(
                        EntityKey.EVENTS,
                        get_entity(EntityKey.EVENTS).get_data_model(),
                    ),
                    selected_columns=[
                        SelectedExpression(
                            "title", Column("_snuba_title", None, "title")
                        ),
                        SelectedExpression(
                            "count", FunctionCall("_snuba_count", "count", tuple())
                        ),
                    ],
                    groupby=[Column("_snuba_title", None, "title")],
                    condition=required_condition,
                ),
                selected_columns=[
                    SelectedExpression(
                        "max_count",
                        FunctionCall(
                            "_snuba_max_count",
                            "max",
                            (Column("_snuba_count", None, "_snuba_count"),),
                        ),
                    ),
                ],
            ),
            selected_columns=[
                SelectedExpression(
                    "min_count",
                    FunctionCall(
                        "_snuba_min_count",
                        "min",
                        (Column("_snuba_max_count", None, "max_count"),),
                    ),
                ),
            ],
            limit=1000,
            offset=0,
        ),
        id="sub query of sub query match",
    ),
    pytest.param(
        f"""MATCH (events) SELECT 4-5,3*foo(c) AS foo,c WHERE a<'stuff\\' "\\" stuff' AND b='"💩\\" \t \\'\\'' AND {added_condition} """,
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(
                        "_snuba_4-5", "minus", (Literal(None, 4), Literal(None, 5))
                    ),
                ),
                SelectedExpression(
                    "3*foo(c) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(c) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "less",
                    Column("_snuba_a", None, "a"),
                    Literal(None, """stuff' "\\" stuff"""),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "equals",
                        Column("_snuba_b", None, "b"),
                        Literal(None, """"💩\\" \t ''"""),
                    ),
                    required_condition,
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="Basic query with crazy characters and escaping",
    ),
    pytest.param(
        f"""MATCH (discover_events )
        SELECT count() AS count BY tags_key
        WHERE or(equals(ifNull(tags[foo],''),'baz'),equals(ifNull(tags[foo.bar],''),'qux'))=1 AND {added_condition}
        ORDER BY count DESC,tags_key ASC  LIMIT 10""",
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "tags_key",
                    Column("_snuba_tags_key", None, "tags_key"),
                ),
                SelectedExpression(
                    "count",
                    FunctionCall("_snuba_count", "count", tuple()),
                ),
            ],
            groupby=[Column("_snuba_tags_key", None, "tags_key")],
            order_by=[
                OrderBy(
                    OrderByDirection.DESC,
                    FunctionCall("_snuba_count", "count", tuple()),
                ),
                OrderBy(
                    OrderByDirection.ASC,
                    Column("_snuba_tags_key", None, "tags_key"),
                ),
            ],
            limit=10,
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    binary_condition(
                        "or",
                        binary_condition(
                            "equals",
                            FunctionCall(
                                None,
                                "ifNull",
                                (
                                    SubscriptableReference(
                                        "_snuba_tags[foo]",
                                        Column("_snuba_tags", None, "tags"),
                                        Literal(None, "foo"),
                                    ),
                                    Literal(None, ""),
                                ),
                            ),
                            Literal(None, "baz"),
                        ),
                        binary_condition(
                            "equals",
                            FunctionCall(
                                None,
                                "ifNull",
                                (
                                    SubscriptableReference(
                                        "_snuba_tags[foo.bar]",
                                        Column("_snuba_tags", None, "tags"),
                                        Literal(None, "foo.bar"),
                                    ),
                                    Literal(None, ""),
                                ),
                            ),
                            Literal(None, "qux"),
                        ),
                    ),
                    Literal(None, 1),
                ),
                required_condition,
            ),
            offset=0,
        ),
        id="Query with nested boolean conditions with multiple empty quoted literals",
    ),
    pytest.param(
        f"""MATCH (discover_events )
        SELECT count() AS times_seen
        WHERE environment = '\\\\\\' \\n \\\\n \\\\\\\\n \\\\\\\\\\n \\\\'
        AND {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "times_seen",
                    FunctionCall("_snuba_times_seen", "count", tuple()),
                ),
            ],
            limit=1000,
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_environment", None, "environment"),
                    Literal(None, "\\' \n \\n \\\\n \\\\\n \\"),
                ),
                required_condition,
            ),
            offset=0,
        ),
        id="Escaping newline cases",
    ),
    pytest.param(
        f"""MATCH (discover_events )
        SELECT count() AS times_seen
        WHERE environment = '\\\\\\\\\\n'
        AND {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "times_seen",
                    FunctionCall("_snuba_times_seen", "count", tuple()),
                ),
            ],
            limit=1000,
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_environment", None, "environment"),
                    Literal(None, "\\\\\n"),
                ),
                required_condition,
            ),
            offset=0,
        ),
        id="Escaping newline cases2",
    ),
    pytest.param(
        f"""MATCH (discover_events )
        SELECT count() AS times_seen
        WHERE environment = 'stuff \\\\\" \\' \\\\\\' stuff\\\\'
        AND {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "times_seen",
                    FunctionCall("_snuba_times_seen", "count", tuple()),
                ),
            ],
            limit=1000,
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_environment", None, "environment"),
                    Literal(None, """stuff \\" ' \\' stuff\\"""),
                ),
                required_condition,
            ),
            offset=0,
        ),
        id="Escaping not newlines cases",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT transaction_name AS tn, tags[release] AS tr, contexts[trace_id] AS cti
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression("tn", Column("_snuba_tn", None, "transaction_name")),
                SelectedExpression(
                    "tr",
                    SubscriptableReference(
                        "_snuba_tr",
                        Column("_snuba_tags", None, "tags"),
                        Literal(None, "release"),
                    ),
                ),
                SelectedExpression(
                    "cti",
                    SubscriptableReference(
                        "_snuba_cti",
                        Column("_snuba_contexts", None, "contexts"),
                        Literal(None, "trace_id"),
                    ),
                ),
            ],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="aliased columns in select",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT count() AS count BY transaction_name AS tn
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression("tn", Column("_snuba_tn", None, "transaction_name")),
                SelectedExpression(
                    "count",
                    FunctionCall("_snuba_count", "count", tuple()),
                ),
            ],
            groupby=[Column("_snuba_tn", None, "transaction_name")],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="aliased columns in select and group by",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT arrayMap((`x`) -> identity(`x`), sdk_integrations) AS sdks
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sdks",
                    FunctionCall(
                        "_snuba_sdks",
                        "arrayMap",
                        (
                            Lambda(
                                None,
                                ("x",),
                                FunctionCall(None, "identity", (Argument(None, "x"),)),
                            ),
                            Column("_snuba_sdk_integrations", None, "sdk_integrations"),
                        ),
                    ),
                ),
            ],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="higher order functions single identifier",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT arrayMap((`x`, `y`) -> identity(tuple(`x`, `y`)), sdk_integrations) AS sdks
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sdks",
                    FunctionCall(
                        "_snuba_sdks",
                        "arrayMap",
                        (
                            Lambda(
                                None,
                                ("x", "y"),
                                FunctionCall(
                                    None,
                                    "identity",
                                    (
                                        FunctionCall(
                                            None,
                                            "tuple",
                                            (Argument(None, "x"), Argument(None, "y")),
                                        ),
                                    ),
                                ),
                            ),
                            Column("_snuba_sdk_integrations", None, "sdk_integrations"),
                        ),
                    ),
                ),
            ],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="higher order function multiple identifier",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT arrayMap((`x`, `y`, `z`) -> tuple(`x`, `y`, `z`), sdk_integrations) AS sdks
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sdks",
                    FunctionCall(
                        "_snuba_sdks",
                        "arrayMap",
                        (
                            Lambda(
                                None,
                                ("x", "y", "z"),
                                FunctionCall(
                                    None,
                                    "tuple",
                                    (
                                        Argument(None, "x"),
                                        Argument(None, "y"),
                                        Argument(None, "z"),
                                    ),
                                ),
                            ),
                            Column("_snuba_sdk_integrations", None, "sdk_integrations"),
                        ),
                    ),
                ),
            ],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="higher order function lots of identifier",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT arrayMap((`x`) -> identity(arrayMap((`y`) -> tuple(`x`, `y`), sdk_integrations)), sdk_integrations) AS sdks
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sdks",
                    FunctionCall(
                        "_snuba_sdks",
                        "arrayMap",
                        (
                            Lambda(
                                None,
                                ("x",),
                                FunctionCall(
                                    None,
                                    "identity",
                                    (
                                        FunctionCall(
                                            None,
                                            "arrayMap",
                                            (
                                                Lambda(
                                                    None,
                                                    ("y",),
                                                    FunctionCall(
                                                        None,
                                                        "tuple",
                                                        (
                                                            Argument(None, "x"),
                                                            Argument(None, "y"),
                                                        ),
                                                    ),
                                                ),
                                                Column(
                                                    "_snuba_sdk_integrations",
                                                    None,
                                                    "sdk_integrations",
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                            Column("_snuba_sdk_integrations", None, "sdk_integrations"),
                        ),
                    ),
                ),
            ],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="higher order function with nested higher order function",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT arrayReduce('sumIf', spans.op, arrayMap((`x`, `y`) -> if(equals(and(equals(`x`, 'db'), equals(`y`, 'ops')), 1), 1, 0), spans.op, spans.group)) AS spans
        WHERE {added_condition}
        """,
        LogicalQuery(
            QueryEntity(
                EntityKey.DISCOVER_EVENTS,
                get_entity(EntityKey.DISCOVER_EVENTS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "spans",
                    FunctionCall(
                        "_snuba_spans",
                        "arrayReduce",
                        (
                            Literal(None, "sumIf"),
                            Column("_snuba_spans.op", None, "spans.op"),
                            FunctionCall(
                                None,
                                "arrayMap",
                                (
                                    Lambda(
                                        None,
                                        ("x", "y"),
                                        FunctionCall(
                                            None,
                                            "if",
                                            (
                                                binary_condition(
                                                    "equals",
                                                    binary_condition(
                                                        "and",
                                                        binary_condition(
                                                            "equals",
                                                            Argument(None, "x"),
                                                            Literal(None, "db"),
                                                        ),
                                                        binary_condition(
                                                            "equals",
                                                            Argument(None, "y"),
                                                            Literal(None, "ops"),
                                                        ),
                                                    ),
                                                    Literal(None, 1),
                                                ),
                                                Literal(None, 1),
                                                Literal(None, 0),
                                            ),
                                        ),
                                    ),
                                    Column("_snuba_spans.op", None, "spans.op"),
                                    Column("_snuba_spans.group", None, "spans.group"),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            limit=1000,
            condition=required_condition,
            offset=0,
        ),
        id="higher order function complex case",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: LogicalQuery) -> None:
    events = get_dataset("events")
    # TODO: Potentially remove this once entities have actual join relationships
    mapping = {
        "contains": (EntityKey.TRANSACTIONS, "event_id"),
        "assigned": (EntityKey.GROUPASSIGNEE, "group_id"),
        "bookmark": (EntityKey.GROUPEDMESSAGE, "first_release_id"),
        "activity": (EntityKey.SESSIONS, "org_id"),
    }

    def events_mock(relationship: str) -> JoinRelationship:
        entity_key, rhs_column = mapping[relationship]
        return JoinRelationship(
            rhs_entity=entity_key,
            join_type=JoinType.INNER,
            columns=[("event_id", rhs_column)],
            equivalences=[],
        )

    events_entity = get_entity(EntityKey.EVENTS)
    setattr(events_entity, "get_join_relationship", events_mock)

    query, _ = parse_snql_query(query_body, events)

    eq, reason = query.equals(expected_query)
    assert eq, reason
