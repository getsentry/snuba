"""
These tests were auto-generated, many of them may be unnecessary or redundant, feel free to remove some.
This tests the first stage of the SnQL parsing pipeline, which looks like SnQL->AST.
"""

from typing import Type

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, literal, or_cond
from snuba.query.logical import Query
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import parse_snql_query_initial

test_cases = [
    pytest.param(
        "MATCH (events) SELECT foo(1) AS `alias`, bar(2) AS `alias`",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("alias", f.foo(literal(1), alias="alias")),
                SelectedExpression("alias", f.bar(literal(2), alias="alias")),
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT test_func(release) AS test_func_alias, event_id BY project_id, platform WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("project_id", column("project_id")),
                SelectedExpression("platform", column("platform")),
                SelectedExpression(
                    "test_func_alias",
                    f.test_func(column("release"), alias="test_func_alias"),
                ),
                SelectedExpression("event_id", column("event_id")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=[column("project_id"), column("platform")],
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count(platform) AS platforms, uniq(platform) AS uniq_platforms, testF(platform, release) AS top_platforms, f1(partition, offset) AS f1_alias, f2() AS f2_alias BY format_eventid(event_id) WHERE tags[sentry:dist] IN tuple('dist1', 'dist2') AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1 HAVING retention_days > 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "format_eventid(event_id)",
                    f.format_eventid(
                        column("event_id"), alias="format_eventid(event_id)"
                    ),
                ),
                SelectedExpression(
                    "platforms", f.count(column("platform"), alias="platforms")
                ),
                SelectedExpression(
                    "uniq_platforms", f.uniq(column("platform"), alias="uniq_platforms")
                ),
                SelectedExpression(
                    "top_platforms",
                    f.testF(
                        column("platform"), column("release"), alias="top_platforms"
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    f.f1(column("partition"), column("offset"), alias="f1_alias"),
                ),
                SelectedExpression("f2_alias", f.f2(alias="f2_alias")),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    in_cond(
                        column("tags[sentry:dist]"),
                        f.tuple(literal("dist1"), literal("dist2")),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=[
                f.format_eventid(column("event_id"), alias="format_eventid(event_id)")
            ],
            having=f.greater(column("retention_days"), literal(1)),
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT partition, offset WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1 ORDER BY partition ASC, offset DESC, func(retention_days) DESC",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("partition", column("partition")),
                SelectedExpression("offset", column("offset")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(OrderByDirection.ASC, column("partition")),
                OrderBy(OrderByDirection.DESC, column("offset")),
                OrderBy(OrderByDirection.DESC, f.func(column("retention_days"))),
            ],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT partition BY platform WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1 ORDER BY partition DESC",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("platform", column("platform")),
                SelectedExpression("partition", column("partition")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=[column("platform")],
            having=None,
            order_by=[OrderBy(OrderByDirection.DESC, column("partition"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT platform, tags[test] BY foo(tags[test2]) WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "foo(tags[test2])",
                    f.foo(column("tags[test2]"), alias="foo(tags[test2])"),
                ),
                SelectedExpression("platform", column("platform")),
                SelectedExpression("tags[test]", column("tags[test]")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=[f.foo(column("tags[test2]"), alias="foo(tags[test2])")],
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT group_id, goo(partition) AS issue_id, foo(zoo(offset)) AS offset WHERE foo(issue_id) AS group_id = 1 AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1 ORDER BY group_id ASC",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("group_id", column("group_id")),
                SelectedExpression(
                    "issue_id", f.goo(column("partition"), alias="issue_id")
                ),
                SelectedExpression(
                    "offset", f.foo(f.zoo(column("offset")), alias="offset")
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(f.foo(column("issue_id"), alias="group_id"), literal(1)),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, column("group_id"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT foo(partition) AS exp, foo(partition) AS exp WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("exp", f.foo(column("partition"), alias="exp")),
                SelectedExpression("exp", f.foo(column("partition"), alias="exp")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT foo(partition) AS exp, exp WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("exp", f.foo(column("partition"), alias="exp")),
                SelectedExpression("exp", column("exp")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, exception_stacks.type ARRAY JOIN exception_stacks.type WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "exception_stacks.type", column("exception_stacks.type")
                ),
            ],
            array_join=[column("exception_stacks.type")],
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, exception_stacks.type WHERE exception_stacks.type LIKE 'Arithmetic%' AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "exception_stacks.type", column("exception_stacks.type")
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.like(column("exception_stacks.type"), literal("Arithmetic%")),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, exception_stacks.type ARRAY JOIN exception_stacks.type WHERE exception_stacks.type LIKE 'Arithmetic%' AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "exception_stacks.type", column("exception_stacks.type")
                ),
            ],
            array_join=[column("exception_stacks.type")],
            condition=and_cond(
                and_cond(
                    f.like(column("exception_stacks.type"), literal("Arithmetic%")),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, arrayJoin(exception_stacks) WHERE exception_stacks.type LIKE 'Arithmetic%' AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "arrayJoin(exception_stacks)",
                    f.arrayJoin(
                        column("exception_stacks"), alias="arrayJoin(exception_stacks)"
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.like(column("exception_stacks.type"), literal("Arithmetic%")),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, exception_stacks.type WHERE or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1 AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "exception_stacks.type", column("exception_stacks.type")
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                column("exception_stacks.type"),
                                literal("ArithmeticException"),
                            ),
                            f.equals(
                                column("exception_stacks.type"),
                                literal("RuntimeException"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, arrayJoin(exception_stacks.type) WHERE or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1 AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "arrayJoin(exception_stacks.type)",
                    f.arrayJoin(
                        column("exception_stacks.type"),
                        alias="arrayJoin(exception_stacks.type)",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                column("exception_stacks.type"),
                                literal("ArithmeticException"),
                            ),
                            f.equals(
                                column("exception_stacks.type"),
                                literal("RuntimeException"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count BY tags_key WHERE or(equals(ifNull(tags[foo], ''), 'baz'), equals(ifNull(tags[foo.bar], ''), 'qux')) = 1 AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("tags_key", column("tags_key")),
                SelectedExpression("count", f.count(alias="count")),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                f.ifNull(column("tags[foo]"), literal("")),
                                literal("baz"),
                            ),
                            f.equals(
                                f.ifNull(column("tags[foo.bar]"), literal("")),
                                literal("qux"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=[column("tags_key")],
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, exception_stacks.type ARRAY JOIN exception_stacks WHERE or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1 AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "exception_stacks.type", column("exception_stacks.type")
                ),
            ],
            array_join=[column("exception_stacks")],
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                column("exception_stacks.type"),
                                literal("ArithmeticException"),
                            ),
                            f.equals(
                                column("exception_stacks.type"),
                                literal("RuntimeException"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT count() AS count, exception_stacks.type ARRAY JOIN exception_stacks WHERE or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1 AND timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="count")),
                SelectedExpression(
                    "exception_stacks.type", column("exception_stacks.type")
                ),
            ],
            array_join=[column("exception_stacks")],
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                column("exception_stacks.type"),
                                literal("ArithmeticException"),
                            ),
                            f.equals(
                                column("exception_stacks.type"),
                                literal("RuntimeException"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT group_id, count(), divide(uniq(tags[url]) AS a+*, 1) BY group_id WHERE timestamp >= toDateTime('2021-01-01T00:00:00') AND timestamp < toDateTime('2021-01-02T00:00:00') AND project_id = 1",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("group_id", column("group_id")),
                SelectedExpression("group_id", column("group_id")),
                SelectedExpression("count()", f.count(alias="count()")),
                SelectedExpression(
                    "divide(uniq(tags[url]) AS a+*, 1)",
                    f.divide(
                        f.uniq(column("tags[url]"), alias="a+*"),
                        literal(1),
                        alias="divide(uniq(tags[url]) AS a+*, 1)",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp"), f.toDateTime(literal("2021-01-01T00:00:00"))
                ),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-02T00:00:00")),
                    ),
                    f.equals(column("project_id"), literal(1)),
                ),
            ),
            groupby=[column("group_id")],
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT f1(column1, column2) AS f1_alias, f2() AS f2_alias, testF(platform, field2) AS f1_alias WHERE project_id = 1 AND timestamp >= toDateTime('2020-01-01 12:00:00') AND timestamp < toDateTime('2020-01-02 12:00:00')",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "f1_alias",
                    f.f1(column("column1"), column("column2"), alias="f1_alias"),
                ),
                SelectedExpression("f2_alias", f.f2(alias="f2_alias")),
                SelectedExpression(
                    "f1_alias",
                    f.testF(column("platform"), column("field2"), alias="f1_alias"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("project_id"), literal(1)),
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2020-01-01 12:00:00")),
                    ),
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2020-01-02 12:00:00")),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT f1(column1, f2) AS f1, f2(f1) AS f2 WHERE project_id = 1 AND timestamp >= toDateTime('2020-01-01 12:00:00') AND timestamp < toDateTime('2020-01-02 12:00:00')",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "f1", f.f1(column("column1"), column("f2"), alias="f1")
                ),
                SelectedExpression("f2", f.f2(column("f1"), alias="f2")),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("project_id"), literal(1)),
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2020-01-01 12:00:00")),
                    ),
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2020-01-02 12:00:00")),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (events) SELECT f1(f2(c) AS f2) AS c WHERE project_id = 1 AND timestamp >= toDateTime('2020-01-01 12:00:00') AND timestamp < toDateTime('2020-01-02 12:00:00')",
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("c", f.f1(f.f2(column("c"), alias="f2"), alias="c"))
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("project_id"), literal(1)),
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2020-01-01 12:00:00")),
                    ),
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2020-01-02 12:00:00")),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        "MATCH (replays) SELECT replay_id BY replay_id WHERE project_id IN array(4552673527463954) AND timestamp < toDateTime('2023-09-22T18:18:10.891157') AND timestamp >= toDateTime('2023-06-24T18:18:10.891157') HAVING or(1, 1, 1, 1) != 0 LIMIT 10",
        Query(
            from_clause=Entity(
                EntityKey.REPLAYS, get_entity(EntityKey.REPLAYS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("replay_id", column("replay_id")),
                SelectedExpression("replay_id", column("replay_id")),
            ],
            array_join=None,
            condition=and_cond(
                in_cond(column("project_id"), f.array(literal(4552673527463954))),
                and_cond(
                    f.less(
                        column("timestamp"),
                        f.toDateTime(literal("2023-09-22T18:18:10.891157")),
                    ),
                    f.greaterOrEquals(
                        column("timestamp"),
                        f.toDateTime(literal("2023-06-24T18:18:10.891157")),
                    ),
                ),
            ),
            groupby=[column("replay_id")],
            having=f.notEquals(
                or_cond(literal(1), literal(1), literal(1), literal(1)), literal(0)
            ),
            order_by=None,
            limitby=None,
            limit=10,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
]


@pytest.mark.parametrize("body, expected", test_cases)
def test_autogenerated(body: str, expected: Query | CompositeQuery[Entity]) -> None:
    actual = parse_snql_query_initial(body)
    eq, reason = actual.equals(expected)
    assert eq, reason


failure_cases = [
    pytest.param(
        "MATCH (events) SELECT f(i(am)bad((at(parentheses)+3() AS `alias`",
        ParsingException,
    ),
]


@pytest.mark.parametrize("body, expected_error", failure_cases)
def test_autogenerated_invalid(body: str, expected_error: Type[Exception]) -> None:
    with pytest.raises(expected_error):
        parse_snql_query_initial(body)
