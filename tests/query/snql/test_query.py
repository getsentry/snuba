import datetime
from unittest import mock

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
        f"MATCH (events) SELECT 4-5, event_id WHERE {added_condition} GRANULARITY 60",
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
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
            ],
            granularity=60,
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="granularity on whole query",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, event_id WHERE {added_condition} TOTALS true",
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
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
            ],
            condition=required_condition,
            totals=True,
            limit=1000,
            offset=0,
        ),
        id="totals on whole query",
    ),
    pytest.param(
        f"MATCH (events SAMPLE 0.5) SELECT 4-5, event_id WHERE {added_condition}",
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
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
            ],
            condition=required_condition,
            sample=0.5,
            limit=1000,
            offset=0,
        ),
        id="sample on entity",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, event_id,title,release WHERE {added_condition} LIMIT 5 BY event_id,title,release",
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
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
                SelectedExpression("title", Column("_snuba_title", None, "title")),
                SelectedExpression(
                    "release", Column("_snuba_release", None, "release")
                ),
            ],
            condition=required_condition,
            limitby=LimitBy(
                5,
                [
                    Column("_snuba_event_id", None, "event_id"),
                    Column("_snuba_title", None, "title"),
                    Column("_snuba_release", None, "release"),
                ],
            ),
            limit=1000,
            offset=0,
        ),
        id="limit by multiple columns",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, group_id WHERE {added_condition} LIMIT 5 BY group_id",
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
                    "group_id", Column("_snuba_group_id", None, "group_id")
                ),
            ],
            condition=required_condition,
            limitby=LimitBy(5, [Column("_snuba_group_id", None, "group_id")]),
            limit=1000,
            offset=0,
        ),
        id="limit by single column",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, event_id WHERE {added_condition} LIMIT 5 OFFSET 3",
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
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
            ],
            condition=required_condition,
            limit=5,
            offset=3,
        ),
        id="limit and offset",
    ),
    pytest.param(
        f"MATCH (events) SELECT 4-5, tags, arrayJoin(tags) AS x WHERE {added_condition} TOTALS true",
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
                SelectedExpression("tags", Column("_snuba_tags", None, "tags")),
                SelectedExpression(
                    "x",
                    FunctionCall(
                        "_snuba_x", "arrayJoin", (Column("_snuba_tags", None, "tags"),)
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
        f"MATCH (events) SELECT 4-5, 3* foo(partition) AS foo, partition WHERE {added_condition}",
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
                    "3* foo(partition) AS foo",
                    FunctionCall(
                        "_snuba_3* foo(partition) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo",
                                "foo",
                                (Column("_snuba_partition", None, "partition"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
            ],
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="Basic query with no spaces and no ambiguous clause content",
    ),
    pytest.param(
        f"""MATCH (events)
        SELECT 4-5,3*foo(partition) AS foo,partition
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
                    "3*foo(partition) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(partition) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo",
                                "foo",
                                (Column("_snuba_partition", None, "partition"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
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
        f"MATCH (events) SELECT count() AS count BY tags[key], contexts[lcp.elementSize] WHERE contexts[lcp.elementSize] > 1 AND {added_condition}",
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
                    "contexts[lcp.elementSize]",
                    SubscriptableReference(
                        "_snuba_contexts[lcp.elementSize]",
                        Column("_snuba_contexts", None, "contexts"),
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
                    "_snuba_contexts[lcp.elementSize]",
                    Column("_snuba_contexts", None, "contexts"),
                    Literal(None, "lcp.elementSize"),
                ),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "greater",
                    SubscriptableReference(
                        "_snuba_contexts[lcp.elementSize]",
                        Column("_snuba_contexts", None, "contexts"),
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
        f"MATCH (events) SELECT (2*(4-5)+3), g(partition) AS goo, partition BY group_id, 2+7 WHERE {added_condition} ORDER BY offset DESC",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id", Column("_snuba_group_id", None, "group_id")
                ),
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
                    FunctionCall(
                        "_snuba_goo",
                        "g",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
            ],
            condition=required_condition,
            groupby=[
                Column("_snuba_group_id", None, "group_id"),
                FunctionCall(
                    "_snuba_2+7", "plus", (Literal(None, 2), Literal(None, 7))
                ),
            ],
            order_by=[
                OrderBy(OrderByDirection.DESC, Column("_snuba_offset", None, "offset"))
            ],
            limit=1000,
            offset=0,
        ),
        id="Simple complete query with example of parenthesized arithmetic expression in SELECT",
    ),
    pytest.param(
        f"MATCH (events) SELECT (2*(4-5)+3), foo(partition) AS thing2, partition BY offset, 2+7 WHERE {added_condition} ORDER BY group_id DESC",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("offset", Column("_snuba_offset", None, "offset")),
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
                        "_snuba_thing2",
                        "foo",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
            ],
            condition=required_condition,
            groupby=[
                Column("_snuba_offset", None, "offset"),
                FunctionCall(
                    "_snuba_2+7", "plus", (Literal(None, 2), Literal(None, 7))
                ),
            ],
            order_by=[
                OrderBy(
                    OrderByDirection.DESC, Column("_snuba_group_id", None, "group_id")
                )
            ],
            limit=1000,
            offset=0,
        ),
        id="Simple complete query with aliased function in SELECT",
    ),
    pytest.param(
        f"MATCH (events) SELECT toDateTime('2020-01-01') AS now, 3*foo(partition) AS foo BY toDateTime('2020-01-01') AS now WHERE {added_condition}",
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
                    "3*foo(partition) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(partition) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo",
                                "foo",
                                (Column("_snuba_partition", None, "partition"),),
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
        f"MATCH (events) SELECT event_id WHERE partition<3 AND offset=2 AND project_id=2 AND group_id=3 AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                )
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "less",
                    Column("_snuba_partition", None, "partition"),
                    Literal(None, 3),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "equals",
                        Column("_snuba_offset", None, "offset"),
                        Literal(None, 2),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "equals",
                            Column("_snuba_project_id", None, "project_id"),
                            Literal(None, 2),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_group_id", None, "group_id"),
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
        f"MATCH (events) SELECT event_id WHERE ((partition<3 OR offset=retention_days) OR title=platform) AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                )
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "or",
                    binary_condition(
                        "or",
                        binary_condition(
                            "less",
                            Column("_snuba_partition", None, "partition"),
                            Literal(None, 3),
                        ),
                        binary_condition(
                            "equals",
                            Column("_snuba_offset", None, "offset"),
                            Column("_snuba_retention_days", None, "retention_days"),
                        ),
                    ),
                    binary_condition(
                        "equals",
                        Column("_snuba_title", None, "title"),
                        Column("_snuba_platform", None, "platform"),
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
        f"MATCH (events) SELECT event_id WHERE (title!=platform OR partition<offset AND (location=gps(user_id,user_name,user_email) OR group_id>0)) AND {added_condition}",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                )
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "or",
                    binary_condition(
                        "notEquals",
                        Column("_snuba_title", None, "title"),
                        Column("_snuba_platform", None, "platform"),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_partition", None, "partition"),
                            Column("_snuba_offset", None, "offset"),
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
                                        Column("_snuba_user_id", None, "user_id"),
                                        Column("_snuba_user_name", None, "user_name"),
                                        Column("_snuba_user_email", None, "user_email"),
                                    ),
                                ),
                            ),
                            binary_condition(
                                "greater",
                                Column("_snuba_group_id", None, "group_id"),
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
        SELECT event_id, tags[test]
        WHERE project_id IN tuple( 2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')""",
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
                SelectedExpression(
                    "tags[test]",
                    SubscriptableReference(
                        "_snuba_tags[test]",
                        Column("_snuba_tags", None, "tags"),
                        key=Literal(None, "test"),
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
        SELECT 4-5,3*foo(partition) AS foo,partition
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
                    "3*foo(partition) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(partition) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo",
                                "foo",
                                (Column("_snuba_partition", None, "partition"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
            ],
            condition=required_condition,
            limit=1000,
            offset=0,
        ),
        id="Basic query with new lines and no ambiguous clause content",
    ),
    pytest.param(
        f"""MATCH (events)
        SELECT 4-5,3*foo(event_id) AS foo,event_id
        WHERE or(equals(arrayExists(exception_stacks.type, '=', 'RuntimeException'), 1), equals(arrayAll(modules.name, 'NOT IN', tuple('Stack', 'Arithmetic')), 1)) = 1 AND {added_condition}""",
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
                    "3*foo(event_id) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(event_id) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo",
                                "foo",
                                (Column("_snuba_event_id", None, "event_id"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
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
                                    Column(
                                        "_snuba_exception_stacks.type",
                                        None,
                                        "exception_stacks.type",
                                    ),
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
                                    Column("_snuba_modules.name", None, "modules.name"),
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
        f"""MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.event_id
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
                SelectedExpression(
                    "e.event_id", Column("_snuba_e.event_id", "e", "event_id")
                ),
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
        f"""MATCH (e: events) -[contains]-> (t: transactions SAMPLE 0.5) SELECT 4-5, t.event_id
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
                SelectedExpression(
                    "t.event_id", Column("_snuba_t.event_id", "t", "event_id")
                ),
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
        SELECT 4-5, ga.offset
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
                SelectedExpression(
                    "ga.offset", Column("_snuba_ga.offset", "ga", "offset")
                ),
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
            (e: events) -[activity]-> (se: metrics_sets)
        SELECT 4-5, e.event_id, t.event_id, ga.offset, gm.offset, se.metric_id
        WHERE {build_cond('e')} AND {build_cond('t')}
        AND se.org_id = 1 AND se.project_id = 1
        AND se.timestamp >= toDateTime('2021-01-01') AND se.timestamp < toDateTime('2021-01-02')""",
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
                                    EntityKey.METRICS_SETS,
                                    get_entity(EntityKey.METRICS_SETS).get_data_model(),
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
                SelectedExpression(
                    "e.event_id", Column("_snuba_e.event_id", "e", "event_id")
                ),
                SelectedExpression(
                    "t.event_id", Column("_snuba_t.event_id", "t", "event_id")
                ),
                SelectedExpression(
                    "ga.offset", Column("_snuba_ga.offset", "ga", "offset")
                ),
                SelectedExpression(
                    "gm.offset", Column("_snuba_gm.offset", "gm", "offset")
                ),
                SelectedExpression(
                    "se.metric_id", Column("_snuba_se.metric_id", "se", "metric_id")
                ),
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
                                                        "_snuba_se.timestamp",
                                                        "se",
                                                        "timestamp",
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
                                                        "_snuba_se.timestamp",
                                                        "se",
                                                        "timestamp",
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
        f"""MATCH (events) SELECT 4-5,3*foo(event_id) AS foo,event_id WHERE title<'stuff\\' "\\" stuff' AND culprit='"💩\\" \t \\'\\'' AND {added_condition} """,
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
                    "3*foo(event_id) AS foo",
                    FunctionCall(
                        "_snuba_3*foo(event_id) AS foo",
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(
                                "_snuba_foo",
                                "foo",
                                (Column("_snuba_event_id", None, "event_id"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "less",
                    Column("_snuba_title", None, "title"),
                    Literal(None, """stuff' "\\" stuff"""),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "equals",
                        Column("_snuba_culprit", None, "culprit"),
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
        "activity": (EntityKey.METRICS_SETS, "org_id"),
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

    with mock.patch.object(events_entity, "get_join_relationship", events_mock):
        query, _ = parse_snql_query(query_body, events)

    eq, reason = query.equals(expected_query)
    assert eq, reason
