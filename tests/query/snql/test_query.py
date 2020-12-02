import datetime
import pytest

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinType,
    JoinClass,
    JoinCondition,
    JoinConditionExpression,
    JoinModifier,
    JoinRelationship,
)
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.parser import parse_snql_query


test_cases = [
    pytest.param(
        "MATCH (e: events) SELECT 4-5, c GRANULARITY 60",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            granularity=60,
        ),
        id="granularity on whole query",
    ),
    pytest.param(
        "MATCH (e:events) SELECT 4-5, c TOTALS true",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            totals=True,
        ),
        id="totals on whole query",
    ),
    pytest.param(
        "MATCH (e: events SAMPLE 0.5) SELECT 4-5, c",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS,
                get_entity(EntityKey.EVENTS).get_data_model(),
                "e",
                0.5,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            sample=0.5,
        ),
        id="sample on entity",
    ),
    pytest.param(
        "MATCH (e: events) SELECT 4-5, c LIMIT 5 BY c",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            limitby=LimitBy(5, Column("_snuba_c", None, "c")),
        ),
        id="limit by column",
    ),
    pytest.param(
        "MATCH (e: events) SELECT 4-5, c LIMIT 5 OFFSET 3",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            limit=5,
            offset=3,
        ),
        id="limit and offset",
    ),
    pytest.param(
        "MATCH (e: events) SELECT 4-5, 3* foo(c) AS foo, c WHERE a<3",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
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
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=binary_condition(
                "less", Column("_snuba_a", None, "a"), Literal(None, 3)
            ),
        ),
        id="Basic query with no spaces and no ambiguous clause content",
    ),
    pytest.param(
        "MATCH (e: events) SELECT count() AS count BY tags[key], measurements[lcp.elementSize] WHERE a<3 AND measurements[lcp.elementSize] > 1",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[
                SelectedExpression(
                    "count", FunctionCall("_snuba_count", "count", tuple()),
                ),
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
                        Column("_snuba_measurements", None, "measurements",),
                        Literal(None, "lcp.elementSize"),
                    ),
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
                    Column("_snuba_measurements", None, "measurements",),
                    Literal(None, "lcp.elementSize"),
                ),
            ],
            condition=binary_condition(
                None,
                "and",
                binary_condition(
                    None, "less", Column("_snuba_a", None, "a"), Literal(None, 3)
                ),
                binary_condition(
                    None,
                    "greater",
                    SubscriptableReference(
                        "_snuba_measurements[lcp.elementSize]",
                        Column("_snuba_measurements", None, "measurements",),
                        Literal(None, "lcp.elementSize"),
                    ),
                    Literal(None, 1),
                ),
            ),
        ),
        id="Basic query with subscriptables",
    ),
    pytest.param(
        "MATCH (e: events) SELECT (2*(4-5)+3), g(c) AS goo, c BY d, 2+7 WHERE a<3  ORDER BY f DESC",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
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
                    "goo",
                    FunctionCall("_snuba_goo", "g", (Column("_snuba_c", None, "c"),)),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
                SelectedExpression("d", Column("_snuba_d", None, "d")),
                SelectedExpression(
                    "2+7",
                    FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
                ),
            ],
            condition=binary_condition(
                "less", Column("_snuba_a", None, "a"), Literal(None, 3)
            ),
            groupby=[
                Column("_snuba_d", None, "d"),
                FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
            ],
            order_by=[OrderBy(OrderByDirection.DESC, Column("_snuba_f", None, "f"))],
        ),
        id="Simple complete query with example of parenthesized arithmetic expression in SELECT",
    ),
    pytest.param(
        "MATCH (e: events) SELECT (2*(4-5)+3), foo(c) AS thing2, c BY d, 2+7 WHERE a<3 ORDER BY f DESC",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
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
                    "thing2",
                    FunctionCall(
                        "_snuba_thing2", "foo", (Column("_snuba_c", None, "c"),)
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
                SelectedExpression("d", Column("_snuba_d", None, "d")),
                SelectedExpression(
                    "2+7",
                    FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
                ),
            ],
            condition=binary_condition(
                "less", Column("_snuba_a", None, "a"), Literal(None, 3)
            ),
            groupby=[
                Column("_snuba_d", None, "d"),
                FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7))),
            ],
            order_by=[OrderBy(OrderByDirection.DESC, Column("_snuba_f", None, "f"))],
        ),
        id="Simple complete query with aliased function in SELECT",
    ),
    pytest.param(
        "MATCH (e:events) SELECT toDateTime('2020-01-01') AS now, 3*foo(c) AS foo BY toDateTime('2020-01-01') AS now WHERE a<3 AND timestamp>toDateTime('2020-01-01')",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[
                SelectedExpression(
                    "now", Literal("_snuba_now", datetime.datetime(2020, 1, 1, 0, 0)),
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
                SelectedExpression(
                    "now", Literal("_snuba_now", datetime.datetime(2020, 1, 1, 0, 0)),
                ),
            ],
            groupby=[Literal("_snuba_now", datetime.datetime(2020, 1, 1, 0, 0))],
            condition=binary_condition(
                "and",
                binary_condition(
                    "less", Column("_snuba_a", None, "a"), Literal(None, 3)
                ),
                binary_condition(
                    "greater",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2020, 1, 1, 0, 0)),
                ),
            ),
        ),
        id="Basic query with date literals",
    ),
    pytest.param(
        "MATCH (e: events) SELECT a WHERE time_seen<3 AND last_seen=2 AND c=2 AND d=3",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
            condition=FunctionCall(
                None,
                "and",
                (
                    FunctionCall(
                        None,
                        "less",
                        (
                            Column("_snuba_time_seen", None, "time_seen"),
                            Literal(None, 3),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "and",
                        (
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column("_snuba_last_seen", None, "last_seen"),
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
                                        (
                                            Column("_snuba_c", None, "c"),
                                            Literal(None, 2),
                                        ),
                                    ),
                                    FunctionCall(
                                        None,
                                        "equals",
                                        (
                                            Column("_snuba_d", None, "d"),
                                            Literal(None, 3),
                                        ),
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
        "MATCH (e: events) SELECT a WHERE (time_seen<3 OR last_seen=afternoon) OR name=bob",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"))],
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
                                    Column("_snuba_time_seen", None, "time_seen"),
                                    Literal(None, 3),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column("_snuba_last_seen", None, "last_seen"),
                                    Column("_snuba_afternoon", None, "afternoon"),
                                ),
                            ),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "equals",
                        (
                            Column("_snuba_name", None, "name"),
                            Column("_snuba_bob", None, "bob"),
                        ),
                    ),
                ),
            ),
        ),
        id="Query with multiple conditions joined by OR / parenthesized OR",
    ),
    pytest.param(
        "MATCH (e: events) SELECT a WHERE name!=bob OR last_seen<afternoon AND (location=gps(x,y,z) OR times_seen>0)",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
            ),
            selected_columns=[SelectedExpression("a", Column("_snuba_a", None, "a"),)],
            condition=FunctionCall(
                None,
                "or",
                (
                    FunctionCall(
                        None,
                        "notEquals",
                        (
                            Column("_snuba_name", None, "name"),
                            Column("_snuba_bob", None, "bob"),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "and",
                        (
                            FunctionCall(
                                None,
                                "less",
                                (
                                    Column("_snuba_last_seen", None, "last_seen"),
                                    Column("_snuba_afternoon", None, "afternoon"),
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
                                    ),
                                    FunctionCall(
                                        None,
                                        "greater",
                                        (
                                            Column(
                                                "_snuba_times_seen", None, "times_seen"
                                            ),
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
        "MATCH (e: events) SELECT a, b[c] WHERE project_id IN tuple( 2 , 3)",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
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
            condition=FunctionCall(
                None,
                "in",
                (
                    Column("_snuba_project_id", None, "project_id",),
                    FunctionCall(None, "tuple", (Literal(None, 2), Literal(None, 3))),
                ),
            ),
        ),
        id="Query with IN condition",
    ),
    pytest.param(
        """MATCH (e:events)
        SELECT 4-5,3*foo(c) AS foo,c
        WHERE a<3""",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
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
                            FunctionCall(
                                "_snuba_foo", "foo", (Column("_snuba_c", None, "c"),)
                            ),
                        ),
                    ),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
            condition=binary_condition(
                "less", Column("_snuba_a", None, "a"), Literal(None, 3)
            ),
        ),
        id="Basic query with new lines and no ambiguous clause content",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: Transactions) SELECT 4-5, c",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS,
                        get_entity(EntityKey.EVENTS).get_data_model(),
                        "e",
                    ),
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                        "t",
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
                join_modifier=JoinModifier.SEMI,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
        ),
        id="Basic join match",
    ),
    pytest.param(
        "MATCH (e: Events) -[contains]-> (t: Transactions SAMPLE 0.5) SELECT 4-5, c",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS,
                        get_entity(EntityKey.EVENTS).get_data_model(),
                        "e",
                    ),
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                        "t",
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
                join_modifier=JoinModifier.SEMI,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
        ),
        id="Basic join match with sample",
    ),
    pytest.param(
        """MATCH
            (e: events) -[contains]-> (t: Transactions),
            (e: events) -[assigned]-> (ga: GroupAssignee)
        SELECT 4-5, c""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=JoinClause(
                    left_node=IndividualNode(
                        "e",
                        QueryEntity(
                            EntityKey.EVENTS,
                            get_entity(EntityKey.EVENTS).get_data_model(),
                            "e",
                        ),
                    ),
                    right_node=IndividualNode(
                        "ga",
                        QueryEntity(
                            EntityKey.GROUPASSIGNEE,
                            get_entity(EntityKey.GROUPASSIGNEE).get_data_model(),
                            "ga",
                        ),
                    ),
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("e", "event_id"),
                            JoinConditionExpression("ga", "group_id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                    join_modifier=JoinModifier.SEMI,
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                        "t",
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
                join_modifier=JoinModifier.SEMI,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
        ),
        id="Multi join match",
    ),
    pytest.param(
        """MATCH
            (e: events) -[contains]-> (t: Transactions),
            (e: events) -[assigned]-> (ga: GroupAssignee),
            (e: events) -[bookmark]-> (gm: GroupedMessage),
            (e: events) -[activity]-> (se: Sessions)
        SELECT 4-5, c""",
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
                                    "e",
                                ),
                            ),
                            right_node=IndividualNode(
                                "se",
                                QueryEntity(
                                    EntityKey.SESSIONS,
                                    get_entity(EntityKey.SESSIONS).get_data_model(),
                                    "se",
                                ),
                            ),
                            keys=[
                                JoinCondition(
                                    JoinConditionExpression("e", "event_id"),
                                    JoinConditionExpression("se", "org_id"),
                                )
                            ],
                            join_type=JoinType.INNER,
                            join_modifier=JoinModifier.SEMI,
                        ),
                        right_node=IndividualNode(
                            "gm",
                            QueryEntity(
                                EntityKey.GROUPEDMESSAGES,
                                get_entity(EntityKey.GROUPEDMESSAGES).get_data_model(),
                                "gm",
                            ),
                        ),
                        keys=[
                            JoinCondition(
                                JoinConditionExpression("e", "event_id"),
                                JoinConditionExpression("gm", "first_release_id"),
                            )
                        ],
                        join_type=JoinType.INNER,
                        join_modifier=JoinModifier.SEMI,
                    ),
                    right_node=IndividualNode(
                        "ga",
                        QueryEntity(
                            EntityKey.GROUPASSIGNEE,
                            get_entity(EntityKey.GROUPASSIGNEE).get_data_model(),
                            "ga",
                        ),
                    ),
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("e", "event_id"),
                            JoinConditionExpression("ga", "group_id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                    join_modifier=JoinModifier.SEMI,
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                        "t",
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
                join_modifier=JoinModifier.SEMI,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("c", Column("_snuba_c", None, "c")),
            ],
        ),
        id="Multi multi join match",
    ),
    pytest.param(
        "MATCH { MATCH (e: events) SELECT count() AS count BY title } SELECT max(count) AS max_count",
        CompositeQuery(
            from_clause=LogicalQuery(
                {},
                QueryEntity(
                    EntityKey.EVENTS,
                    get_entity(EntityKey.EVENTS).get_data_model(),
                    "e",
                ),
                selected_columns=[
                    SelectedExpression(
                        "count", FunctionCall("_snuba_count", "count", tuple())
                    ),
                    SelectedExpression("title", Column("_snuba_title", None, "title")),
                ],
                groupby=[Column("_snuba_title", None, "title")],
            ),
            selected_columns=[
                SelectedExpression(
                    "max_count",
                    FunctionCall(
                        "_snuba_max_count",
                        "max",
                        (Column("_snuba_count", None, "count"),),
                    ),
                ),
            ],
        ),
        id="sub query match",
    ),
    pytest.param(
        "MATCH { MATCH { MATCH (e: events) SELECT count() AS count BY title } SELECT max(count) AS max_count } SELECT min(count) AS min_count",
        CompositeQuery(
            from_clause=CompositeQuery(
                from_clause=LogicalQuery(
                    {},
                    QueryEntity(
                        EntityKey.EVENTS,
                        get_entity(EntityKey.EVENTS).get_data_model(),
                        "e",
                    ),
                    selected_columns=[
                        SelectedExpression(
                            "count", FunctionCall("_snuba_count", "count", tuple())
                        ),
                        SelectedExpression(
                            "title", Column("_snuba_title", None, "title")
                        ),
                    ],
                    groupby=[Column("_snuba_title", None, "title")],
                ),
                selected_columns=[
                    SelectedExpression(
                        "max_count",
                        FunctionCall(
                            "_snuba_max_count",
                            "max",
                            (Column("_snuba_count", None, "count"),),
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
                        (Column("_snuba_count", None, "count"),),
                    ),
                ),
            ],
        ),
        id="sub query of sub query match",
    ),
    pytest.param(
        """MATCH (e:events) SELECT 4-5,3*foo(c) AS foo,c WHERE a<'stuff\\' "\\" stuff' AND b='"💩\\" \t \\'\\''""",
        LogicalQuery(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(), "e",
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
                    "equals",
                    Column("_snuba_b", None, "b"),
                    Literal(None, """"💩\\" \t ''"""),
                ),
            ),
        ),
        id="Basic query with crazy characters and escaping",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: LogicalQuery) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")

    # TODO: Potentially remove this once entities have actual join relationships
    mapping = {
        "contains": (EntityKey.TRANSACTIONS, "event_id"),
        "assigned": (EntityKey.GROUPASSIGNEE, "group_id"),
        "bookmark": (EntityKey.GROUPEDMESSAGES, "first_release_id"),
        "activity": (EntityKey.SESSIONS, "org_id"),
    }

    def events_mock(relationship: str) -> JoinRelationship:
        entity_key, rhs_column = mapping[relationship]
        return JoinRelationship(
            rhs_entity=entity_key,
            join_class=JoinClass.ONE_2_ONE,
            join_type=JoinType.INNER,
            join_modifier=JoinModifier.SEMI,
            columns=[("event_id", rhs_column)],
        )

    events_entity = get_entity(EntityKey.EVENTS)
    setattr(events_entity, "get_join_relationship", events_mock)

    query = parse_snql_query(query_body, events)

    eq, reason = query.equals(expected_query)
    assert eq, reason
