from datetime import datetime
from typing import Any, Dict, Optional

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
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
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query
from snuba.query.parser.exceptions import AliasShadowingException, CyclicAliasException
from snuba.query.snql.parser import parse_snql_query


def with_required(condition: Optional[Expression] = None) -> Expression:
    required = binary_condition(
        BooleanFunctions.AND,
        FunctionCall(
            None,
            "greaterOrEquals",
            (
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "less",
                (
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime(2021, 1, 2, 0, 0)),
                ),
            ),
            FunctionCall(
                None,
                "equals",
                (
                    Column("_snuba_project_id", None, "project_id"),
                    Literal(None, 1),
                ),
            ),
        ),
    )

    if condition:
        return binary_condition(BooleanFunctions.AND, condition, required)

    return required


def with_required_mql(condition: Optional[Expression] = None) -> Expression:
    required = binary_condition(
        BooleanFunctions.AND,
        FunctionCall(
            None,
            "in",
            (
                Column("_snuba_project_id", None, "project_id"),
                FunctionCall(None, "tuple", (Literal(None, 1),)),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "in",
                (
                    Column("_snuba_org_id", None, "org_id"),
                    FunctionCall(None, "tuple", (Literal(None, 1),)),
                ),
            ),
            binary_condition(
                BooleanFunctions.AND,
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column("_snuba_use_case_id", None, "use_case_id"),
                        Literal(None, "transactions"),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    FunctionCall(
                        None,
                        "greaterOrEquals",
                        (
                            Column("_snuba_timestamp", None, "timestamp"),
                            Literal(None, datetime(2021, 1, 1, 0, 0)),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "less",
                        (
                            Column("_snuba_timestamp", None, "timestamp"),
                            Literal(None, datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                ),
            ),
        ),
    )

    if condition:
        return binary_condition(BooleanFunctions.AND, condition, required)

    return required


DEFAULT_TEST_QUERY_CONDITIONS = [
    "timestamp >= toDateTime('2021-01-01T00:00:00')",
    "timestamp < toDateTime('2021-01-02T00:00:00')",
    "project_id = 1",
]


def snql_conditions_with_default(*conditions: str) -> str:
    return " AND ".join(list(conditions) + DEFAULT_TEST_QUERY_CONDITIONS)


snql_test_cases = [
    pytest.param(
        """
           MATCH (events)
           SELECT test_func(release) AS test_func_alias,
              event_id BY project_id, platform
           WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "project_id", Column("_snuba_project_id", None, "project_id")
                ),
                SelectedExpression(
                    "platform", Column("_snuba_platform", None, "platform")
                ),
                SelectedExpression(
                    "test_func_alias",
                    FunctionCall(
                        "_snuba_test_func_alias",
                        "test_func",
                        (Column("_snuba_release", None, "release"),),
                    ),
                ),
                SelectedExpression(
                    "event_id", Column("_snuba_event_id", None, "event_id")
                ),
            ],
            groupby=[
                Column("_snuba_project_id", None, "project_id"),
                Column("_snuba_platform", None, "platform"),
            ],
            condition=with_required(),
            limit=1000,
        ),
        id="Select composed by select, groupby and aggregations",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count(platform) AS platforms,
               uniq(platform) AS uniq_platforms,
               testF(platform, release) AS top_platforms,
               f1(partition, offset) AS f1_alias, f2() AS f2_alias
        BY format_eventid(event_id)
        WHERE {conditions}
        HAVING retention_days > 1
        """.format(
            conditions=snql_conditions_with_default(
                "tags[sentry:dist] IN tuple('dist1', 'dist2')"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "format_eventid(event_id)",
                    FunctionCall(
                        "_snuba_format_eventid(event_id)",
                        "format_eventid",
                        (Column("_snuba_event_id", None, "event_id"),),
                    ),
                ),
                SelectedExpression(
                    "platforms",
                    FunctionCall(
                        "_snuba_platforms",
                        "count",
                        (Column("_snuba_platform", None, "platform"),),
                    ),
                ),
                SelectedExpression(
                    "uniq_platforms",
                    FunctionCall(
                        "_snuba_uniq_platforms",
                        "uniq",
                        (Column("_snuba_platform", None, "platform"),),
                    ),
                ),
                SelectedExpression(
                    "top_platforms",
                    FunctionCall(
                        "_snuba_top_platforms",
                        "testF",
                        (
                            Column("_snuba_platform", None, "platform"),
                            Column("_snuba_release", None, "release"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    FunctionCall(
                        "_snuba_f1_alias",
                        "f1",
                        (
                            Column("_snuba_partition", None, "partition"),
                            Column("_snuba_offset", None, "offset"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f2_alias", FunctionCall("_snuba_f2_alias", "f2", ())
                ),
            ],
            condition=with_required(
                binary_condition(
                    "in",
                    SubscriptableReference(
                        "_snuba_tags[sentry:dist]",
                        Column("_snuba_tags", None, "tags"),
                        Literal(None, "sentry:dist"),
                    ),
                    FunctionCall(
                        None,
                        "tuple",
                        (
                            Literal(None, "dist1"),
                            Literal(None, "dist2"),
                        ),
                    ),
                )
            ),
            having=binary_condition(
                "greater",
                Column("_snuba_retention_days", None, "retention_days"),
                Literal(None, 1),
            ),
            groupby=[
                FunctionCall(
                    "_snuba_format_eventid(event_id)",
                    "format_eventid",
                    (Column("_snuba_event_id", None, "event_id"),),
                )
            ],
            limit=1000,
        ),
        id="Format a query with functions in all fields",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT partition, offset
        WHERE {conditions}
        ORDER BY partition ASC,
                 offset DESC,
                 func(retention_days) DESC
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
                SelectedExpression("offset", Column("_snuba_offset", None, "offset")),
            ],
            condition=with_required(),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC, Column("_snuba_partition", None, "partition")
                ),
                OrderBy(OrderByDirection.DESC, Column("_snuba_offset", None, "offset")),
                OrderBy(
                    OrderByDirection.DESC,
                    FunctionCall(
                        None,
                        "func",
                        (Column("_snuba_retention_days", None, "retention_days"),),
                    ),
                ),
            ],
            limit=1000,
        ),
        id="Order by with functions",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT partition BY platform
        WHERE {conditions}
        ORDER BY partition DESC
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "platform", Column("_snuba_platform", None, "platform")
                ),
                SelectedExpression(
                    "partition", Column("_snuba_partition", None, "partition")
                ),
            ],
            condition=with_required(),
            groupby=[Column("_snuba_platform", None, "platform")],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.DESC, Column("_snuba_partition", None, "partition")
                )
            ],
            limit=1000,
        ),
        id="Order and group by provided as string",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT platform, tags[test] BY foo(tags[test2])
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    name="foo(tags[test2])",
                    expression=FunctionCall(
                        "_snuba_foo(tags[test2])",
                        "foo",
                        (
                            SubscriptableReference(
                                "_snuba_tags[test2]",
                                Column("_snuba_tags", None, "tags"),
                                Literal(None, "test2"),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "platform", Column("_snuba_platform", None, "platform")
                ),
                SelectedExpression(
                    "tags[test]",
                    SubscriptableReference(
                        "_snuba_tags[test]",
                        Column("_snuba_tags", None, "tags"),
                        Literal(None, "test"),
                    ),
                ),
            ],
            groupby=[
                FunctionCall(
                    "_snuba_foo(tags[test2])",
                    "foo",
                    (
                        SubscriptableReference(
                            "_snuba_tags[test2]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "test2"),
                        ),
                    ),
                )
            ],
            condition=with_required(),
            limit=1000,
        ),
        id="Unpacks subscriptable references",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT group_id, goo(partition) AS issue_id,
               foo(zoo(offset)) AS offset
        WHERE {conditions}
        ORDER BY group_id ASC
        """.format(
            conditions=snql_conditions_with_default("foo(issue_id) AS group_id = 1")
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id",
                    FunctionCall(
                        "_snuba_group_id",
                        "foo",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "goo",
                                (Column("_snuba_partition", None, "partition"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "issue_id",
                    FunctionCall(
                        "_snuba_issue_id",
                        "goo",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
                SelectedExpression(
                    "offset",
                    FunctionCall(
                        "_snuba_offset",
                        "foo",
                        (FunctionCall(None, "zoo", (Column(None, None, "offset"),)),),
                    ),
                ),
            ],
            condition=with_required(
                binary_condition(
                    "equals",
                    FunctionCall(
                        "_snuba_group_id",
                        "foo",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "goo",
                                (Column("_snuba_partition", None, "partition"),),
                            ),
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(
                        "_snuba_group_id",
                        "foo",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "goo",
                                (Column("_snuba_partition", None, "partition"),),
                            ),
                        ),
                    ),
                ),
            ],
            limit=1000,
        ),
        id="Alias references are expanded",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT foo(partition) AS exp,
               foo(partition) AS exp
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp",
                        "foo",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp",
                        "foo",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
            ],
            condition=with_required(),
            limit=1000,
        ),
        id="Allowed duplicate alias (same expression)",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT foo(partition) AS exp, exp
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp",
                        "foo",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp",
                        "foo",
                        (Column("_snuba_partition", None, "partition"),),
                    ),
                ),
            ],
            condition=with_required(),
            limit=1000,
        ),
        id="De-escape aliases defined by the user",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count,
               exception_stacks.type
        ARRAY JOIN exception_stacks.type
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            array_join=[Column("exception_stacks.type", None, "exception_stacks.type")],
            condition=with_required(),
            limit=1000,
        ),
        id="Format a query with array join",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count,
            exception_stacks.type
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "exception_stacks.type LIKE 'Arithmetic%'"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=with_required(
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
                                        "like",
                                        (
                                            Argument(None, "x"),
                                            Literal(None, "Arithmetic%"),
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
                )
            ),
            limit=1000,
        ),
        id="Format a query with array field in a condition",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count,
           exception_stacks.type
        ARRAY JOIN exception_stacks.type
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "exception_stacks.type LIKE 'Arithmetic%'"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            array_join=[Column("exception_stacks.type", None, "exception_stacks.type")],
            condition=with_required(
                FunctionCall(
                    None,
                    "like",
                    (
                        Column(
                            "_snuba_exception_stacks.type",
                            None,
                            "exception_stacks.type",
                        ),
                        Literal(None, "Arithmetic%"),
                    ),
                )
            ),
            limit=1000,
        ),
        id="Format a query with array join field in a condition",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count,
             arrayJoin(exception_stacks)
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "exception_stacks.type LIKE 'Arithmetic%'"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "arrayJoin(exception_stacks)",
                    FunctionCall(
                        "_snuba_arrayJoin(exception_stacks)",
                        "arrayJoin",
                        (Column("_snuba_exception_stacks", None, "exception_stacks"),),
                    ),
                ),
            ],
            condition=with_required(
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
                                        "like",
                                        (
                                            Argument(None, "x"),
                                            Literal(None, "Arithmetic%"),
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
            ),
            limit=1000,
        ),
        id="Format a query with array join field in a condition and array join in a function",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count,
          exception_stacks.type
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=with_required(
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(
                        None,
                        "or",
                        (
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
                                                            None,
                                                            "ArithmeticException",
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
                                                            None,
                                                            "RuntimeException",
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
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            limit=1000,
        ),
        id="Format a query with array field in a boolean condition",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count,
          arrayJoin(exception_stacks.type)
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "arrayJoin(exception_stacks.type)",
                    FunctionCall(
                        "_snuba_arrayJoin(exception_stacks.type)",
                        "arrayJoin",
                        (
                            Column(
                                "_snuba_exception_stacks.type",
                                None,
                                "exception_stacks.type",
                            ),
                        ),
                    ),
                ),
            ],
            condition=with_required(
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(
                        None,
                        "or",
                        (
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
                                                            None,
                                                            "ArithmeticException",
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
                                                            None,
                                                            "RuntimeException",
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
                        ),
                    ),
                    Literal(alias=None, value=1),
                ),
            ),
            limit=1000,
        ),
        id="Format a query with array join field in a boolean condition and array join in a function",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count BY tags_key
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "or(equals(ifNull(tags[foo], ''), 'baz'), equals(ifNull(tags[foo.bar], ''), 'qux')) = 1"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    name="tags_key",
                    expression=Column("_snuba_tags_key", None, "tags_key"),
                ),
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
            ],
            groupby=[Column("_snuba_tags_key", None, "tags_key")],
            condition=with_required(
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(
                        None,
                        "or",
                        (
                            FunctionCall(
                                None,
                                "equals",
                                (
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
                            ),
                            FunctionCall(
                                None,
                                "equals",
                                (
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
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            limit=1000,
        ),
        id="Format a query with array column nested in function",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count, exception_stacks.type
        ARRAY JOIN exception_stacks
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=with_required(
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(
                        None,
                        "or",
                        (
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column(
                                        "_snuba_exception_stacks.type",
                                        None,
                                        "exception_stacks.type",
                                    ),
                                    Literal(None, "ArithmeticException"),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column(
                                        "_snuba_exception_stacks.type",
                                        None,
                                        "exception_stacks.type",
                                    ),
                                    Literal(None, "RuntimeException"),
                                ),
                            ),
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            limit=1000,
            array_join=[Column("exception_stacks", None, "exception_stacks")],
        ),
        id="Format a query with array join field in a boolean condition",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT count() AS count, exception_stacks.type
        ARRAY JOIN exception_stacks, hierarchical_hashes
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default(
                "or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1"
            )
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=with_required(
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(
                        None,
                        "or",
                        (
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column(
                                        "_snuba_exception_stacks.type",
                                        None,
                                        "exception_stacks.type",
                                    ),
                                    Literal(None, "ArithmeticException"),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column(
                                        "_snuba_exception_stacks.type",
                                        None,
                                        "exception_stacks.type",
                                    ),
                                    Literal(None, "RuntimeException"),
                                ),
                            ),
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            limit=1000,
            array_join=[
                Column("exception_stacks", None, "exception_stacks"),
                Column("hierarchical_hashes", None, "hierarchical_hashes"),
            ],
        ),
        id="Format a query with 2 array join fields in a boolean condition",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT group_id, count(), divide(uniq(tags[url]) AS a+*, 1)
        BY group_id
        WHERE {conditions}
        """.format(
            conditions=snql_conditions_with_default()
        ),
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id",
                    Column("_snuba_group_id", None, "group_id"),
                ),
                SelectedExpression(
                    "group_id",
                    Column("_snuba_group_id", None, "group_id"),
                ),
                SelectedExpression(
                    "count()", FunctionCall("_snuba_count()", "count", tuple())
                ),
                SelectedExpression(
                    "divide(uniq(tags[url]) AS a+*, 1)",
                    FunctionCall(
                        "_snuba_divide(uniq(tags[url]) AS a+*, 1)",
                        "divide",
                        (
                            FunctionCall(
                                "_snuba_a+*",
                                "uniq",
                                (
                                    SubscriptableReference(
                                        "_snuba_tags[url]",
                                        Column("_snuba_tags", None, "tags"),
                                        Literal(None, "url"),
                                    ),
                                ),
                            ),
                            Literal(None, 1),
                        ),
                    ),
                ),
            ],
            groupby=[Column("_snuba_group_id", None, "group_id")],
            condition=with_required(),
            limit=1000,
        ),
        id="Format a query with expressions without aliases",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", snql_test_cases)
def test_format_expressions_from_snql(query_body: str, expected_query: Query) -> None:
    events = get_dataset("events")
    query, _ = parse_snql_query(str(query_body), events)

    eq, reason = query.equals(expected_query)
    assert eq, reason


def test_shadowing() -> None:
    with pytest.raises(AliasShadowingException):
        parse_snql_query(
            """
            MATCH (events)
            SELECT f1(column1, column2) AS f1_alias, f2() AS f2_alias, testF(platform, field2) AS f1_alias
            WHERE project_id = 1
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            """,
            get_dataset("events"),
        )


def test_circular_aliases() -> None:
    with pytest.raises(CyclicAliasException):
        parse_snql_query(
            """
            MATCH (events)
            SELECT f1(column1, f2) AS f1, f2(f1) AS f2
            WHERE project_id = 1
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            """,
            get_dataset("events"),
        )

    with pytest.raises(CyclicAliasException):
        parse_snql_query(
            """
            MATCH (events)
            SELECT f1(f2(c) AS f2) AS c
            WHERE project_id = 1
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            """,
            get_dataset("events"),
        )


def test_treeify() -> None:
    query = """MATCH (replays)
    SELECT replay_id BY replay_id
    WHERE project_id IN array(4552673527463954) AND timestamp < toDateTime('2023-09-22T18:18:10.891157') AND timestamp >= toDateTime('2023-06-24T18:18:10.891157')
    HAVING or(1, 1, 1, 1) != 0 LIMIT 10
    """
    query_ast, _ = parse_snql_query(query, get_dataset("replays"))
    having = query_ast.get_having()
    expected = binary_condition(
        ConditionFunctions.NEQ,
        binary_condition(
            BooleanFunctions.OR,
            Literal(None, 1),
            binary_condition(
                BooleanFunctions.OR,
                Literal(None, 1),
                binary_condition(
                    BooleanFunctions.OR,
                    Literal(None, 1),
                    Literal(None, 1),
                ),
            ),
        ),
        Literal(None, 0),
    )
    assert having == expected


mql_test_cases = [
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": [{"column_name": "timestamp", "direction": "ASC"}],
                "granularity": "60",
                "interval": "60",
                "with_totals": "",
            },
            "scope": {
                "org_ids": ["1"],
                "project_ids": ["1"],
                "use_case_id": "transactions",
            },
            "limit": "",
            "offset": "",
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        Query(
            QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        None,
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            groupby=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                FunctionCall(
                    None,
                    "in",
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        FunctionCall(None, "tuple", (Literal(None, 1),)),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    FunctionCall(
                        None,
                        "in",
                        (
                            Column("_snuba_org_id", None, "org_id"),
                            FunctionCall(None, "tuple", (Literal(None, 1),)),
                        ),
                    ),
                    binary_condition(
                        BooleanFunctions.AND,
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column("_snuba_use_case_id", None, "use_case_id"),
                                Literal(None, "transactions"),
                            ),
                        ),
                        binary_condition(
                            BooleanFunctions.AND,
                            FunctionCall(
                                None,
                                "greaterOrEquals",
                                (
                                    Column("_snuba_timestamp", None, "timestamp"),
                                    Literal(None, datetime(2021, 1, 1, 0, 0)),
                                ),
                            ),
                            binary_condition(
                                BooleanFunctions.AND,
                                FunctionCall(
                                    None,
                                    "less",
                                    (
                                        Column("_snuba_timestamp", None, "timestamp"),
                                        Literal(None, datetime(2021, 1, 2, 0, 0)),
                                    ),
                                ),
                                binary_condition(
                                    BooleanFunctions.AND,
                                    FunctionCall(
                                        None,
                                        "equals",
                                        (
                                            Column(
                                                "_snuba_metric_id", None, "metric_id"
                                            ),
                                            Literal(None, "123456"),
                                        ),
                                    ),
                                    FunctionCall(
                                        None,
                                        "in",
                                        (
                                            SubscriptableReference(
                                                "_snuba_tags_raw[000888]",
                                                Column(
                                                    "_snuba_tags_raw", None, "tags_raw"
                                                ),
                                                Literal(None, "000888"),
                                            ),
                                            FunctionCall(
                                                None,
                                                "tuple",
                                                (
                                                    Literal(None, "dist1"),
                                                    Literal(None, "dist2"),
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
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    Column(
                        alias="_snuba_timestamp",
                        table_name=None,
                        column_name="timestamp",
                    ),
                )
            ],
            limit=1000,
            granularity=60,
        ),
        id="Select metric with filter",
    ),
    pytest.param(
        'max(transaction.user{!dist:["dist1", "dist2"]}){foo: bar} by transaction',
        {
            "entity": "generic_metrics_sets",
            "start": "2021-01-01T01:36:00",
            "end": "2021-01-05T04:15:00",
            "rollup": {
                "orderby": [],
                "granularity": "3600",
                "interval": "60",
                "with_totals": "",
            },
            "scope": {
                "org_ids": ["1"],
                "project_ids": ["1"],
                "use_case_id": "transactions",
            },
            "limit": "100",
            "offset": "3",
            "indexer_mappings": {
                "transaction.user": "s:transactions/user@none",
                "s:transactions/user@none": "567890",
                "dist": "000888",
                "foo": "000777",
                "transaction": "111111",
            },
        },
        Query(
            QueryEntity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "max(transaction.user)",
                    FunctionCall(
                        None,
                        "max",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
                SelectedExpression(
                    "transaction",
                    SubscriptableReference(
                        "_snuba_tags_raw[111111]",
                        Column(
                            "_snuba_tags_raw",
                            None,
                            "tags_raw",
                        ),
                        Literal(None, "111111"),
                    ),
                ),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                FunctionCall(
                    None,
                    "in",
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        FunctionCall(None, "tuple", (Literal(None, 1),)),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    FunctionCall(
                        None,
                        "in",
                        (
                            Column("_snuba_org_id", None, "org_id"),
                            FunctionCall(None, "tuple", (Literal(None, 1),)),
                        ),
                    ),
                    binary_condition(
                        BooleanFunctions.AND,
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column("_snuba_use_case_id", None, "use_case_id"),
                                Literal(None, "transactions"),
                            ),
                        ),
                        binary_condition(
                            BooleanFunctions.AND,
                            FunctionCall(
                                None,
                                "greaterOrEquals",
                                (
                                    Column("_snuba_timestamp", None, "timestamp"),
                                    Literal(None, datetime(2021, 1, 1, 1, 36)),
                                ),
                            ),
                            binary_condition(
                                BooleanFunctions.AND,
                                FunctionCall(
                                    None,
                                    "less",
                                    (
                                        Column("_snuba_timestamp", None, "timestamp"),
                                        Literal(None, datetime(2021, 1, 5, 4, 15)),
                                    ),
                                ),
                                binary_condition(
                                    BooleanFunctions.AND,
                                    FunctionCall(
                                        None,
                                        "equals",
                                        (
                                            Column(
                                                "_snuba_metric_id", None, "metric_id"
                                            ),
                                            Literal(None, "567890"),
                                        ),
                                    ),
                                    binary_condition(
                                        BooleanFunctions.AND,
                                        FunctionCall(
                                            None,
                                            "notIn",
                                            (
                                                SubscriptableReference(
                                                    "_snuba_tags_raw[000888]",
                                                    Column(
                                                        "_snuba_tags_raw",
                                                        None,
                                                        "tags_raw",
                                                    ),
                                                    Literal(None, "000888"),
                                                ),
                                                FunctionCall(
                                                    None,
                                                    "tuple",
                                                    (
                                                        Literal(None, "dist1"),
                                                        Literal(None, "dist2"),
                                                    ),
                                                ),
                                            ),
                                        ),
                                        FunctionCall(
                                            None,
                                            "equals",
                                            (
                                                SubscriptableReference(
                                                    "_snuba_tags_raw[000777]",
                                                    Column(
                                                        "_snuba_tags_raw",
                                                        None,
                                                        "tags_raw",
                                                    ),
                                                    Literal(None, "000777"),
                                                ),
                                                Literal(None, "bar"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            order_by=[],
            groupby=[
                SubscriptableReference(
                    "_snuba_tags_raw[111111]",
                    Column(
                        "_snuba_tags_raw",
                        None,
                        "tags_raw",
                    ),
                    Literal(None, "111111"),
                )
            ],
            limit=100,
            offset=3,
            granularity=3600,
        ),
        id="Select metric with filter and groupby",
    ),
]


@pytest.mark.parametrize("query_body, mql_context, expected_query", mql_test_cases)
def test_format_expressions_from_mql(
    query_body: str, mql_context: Dict[str, Any], expected_query: Query
) -> None:
    generic_metrics = get_dataset("generic_metrics")
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)

    eq, reason = query.equals(expected_query)
    assert eq, reason
