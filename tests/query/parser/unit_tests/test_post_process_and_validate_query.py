"""
These tests were auto-generated, many of them may be unnecessary or redundant, feel free to remove some.
This tests the final stage of the SnQL parsing pipeline, which looks like AST->AST.
"""

from datetime import datetime
from typing import Type

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, LogicalDataSource
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, and_cond, column, in_cond, literal, or_cond
from snuba.query.expressions import Argument, Lambda
from snuba.query.logical import Query
from snuba.query.parser.exceptions import AliasShadowingException, CyclicAliasException
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import CustomProcessors, PostProcessAndValidateQuery
from snuba.utils.metrics.timer import Timer

tags = NestedColumn("tags")
tags_raw = NestedColumn("tags_raw")

test_cases = [
    pytest.param(
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "project_id", column("project_id", None, "_snuba_project_id")
                ),
                SelectedExpression(
                    "platform", column("platform", None, "_snuba_platform")
                ),
                SelectedExpression(
                    "test_func_alias",
                    f.test_func(
                        column("release", None, "_snuba_release"),
                        alias="_snuba_test_func_alias",
                    ),
                ),
                SelectedExpression(
                    "event_id", column("event_id", None, "_snuba_event_id")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=[
                column("project_id", None, "_snuba_project_id"),
                column("platform", None, "_snuba_platform"),
            ],
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
        (
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
                        "uniq_platforms",
                        f.uniq(column("platform"), alias="uniq_platforms"),
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
                    in_cond(
                        column("tags[sentry:dist]"),
                        f.tuple(literal("dist1"), literal("dist2")),
                    ),
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
                        ),
                    ),
                ),
                groupby=[
                    f.format_eventid(
                        column("event_id"), alias="format_eventid(event_id)"
                    )
                ],
                having=f.greater(column("retention_days"), literal(1)),
                order_by=None,
                limitby=None,
                limit=1000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "format_eventid(event_id)",
                    f.format_eventid(
                        column("event_id", None, "_snuba_event_id"),
                        alias="_snuba_format_eventid(event_id)",
                    ),
                ),
                SelectedExpression(
                    "platforms",
                    f.count(
                        column("platform", None, "_snuba_platform"),
                        alias="_snuba_platforms",
                    ),
                ),
                SelectedExpression(
                    "uniq_platforms",
                    f.uniq(
                        column("platform", None, "_snuba_platform"),
                        alias="_snuba_uniq_platforms",
                    ),
                ),
                SelectedExpression(
                    "top_platforms",
                    f.testF(
                        column("platform", None, "_snuba_platform"),
                        column("release", None, "_snuba_release"),
                        alias="_snuba_top_platforms",
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    f.f1(
                        column("partition", None, "_snuba_partition"),
                        column("offset", None, "_snuba_offset"),
                        alias="_snuba_f1_alias",
                    ),
                ),
                SelectedExpression("f2_alias", f.f2(alias="_snuba_f2_alias")),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    in_cond(
                        tags["sentry:dist"], f.tuple(literal("dist1"), literal("dist2"))
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=[
                f.format_eventid(
                    column("event_id", None, "_snuba_event_id"),
                    alias="_snuba_format_eventid(event_id)",
                )
            ],
            having=f.greater(
                column("retention_days", None, "_snuba_retention_days"), literal(1)
            ),
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "partition", column("partition", None, "_snuba_partition")
                ),
                SelectedExpression("offset", column("offset", None, "_snuba_offset")),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC, column("partition", None, "_snuba_partition")
                ),
                OrderBy(OrderByDirection.DESC, column("offset", None, "_snuba_offset")),
                OrderBy(
                    OrderByDirection.DESC,
                    f.func(column("retention_days", None, "_snuba_retention_days")),
                ),
            ],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "platform", column("platform", None, "_snuba_platform")
                ),
                SelectedExpression(
                    "partition", column("partition", None, "_snuba_partition")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=[column("platform", None, "_snuba_platform")],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.DESC, column("partition", None, "_snuba_partition")
                )
            ],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "foo(tags[test2])",
                    f.foo(tags["test2"], alias="_snuba_foo(tags[test2])"),
                ),
                SelectedExpression(
                    "platform", column("platform", None, "_snuba_platform")
                ),
                SelectedExpression("tags[test]", tags["test"]),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=[f.foo(tags["test2"], alias="_snuba_foo(tags[test2])")],
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
        (
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
                    f.equals(f.foo(column("issue_id"), alias="group_id"), literal(1)),
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
                        ),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id",
                    f.foo(
                        f.goo(
                            column("partition", None, "_snuba_partition"),
                            alias="_snuba_issue_id",
                        ),
                        alias="_snuba_group_id",
                    ),
                ),
                SelectedExpression(
                    "issue_id",
                    f.goo(
                        column("partition", None, "_snuba_partition"),
                        alias="_snuba_issue_id",
                    ),
                ),
                SelectedExpression(
                    "offset", f.foo(f.zoo(column("offset")), alias="_snuba_offset")
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        f.foo(
                            f.goo(
                                column("partition", None, "_snuba_partition"),
                                alias="_snuba_issue_id",
                            ),
                            alias="_snuba_group_id",
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.foo(
                        f.goo(
                            column("partition", None, "_snuba_partition"),
                            alias="_snuba_issue_id",
                        ),
                        alias="_snuba_group_id",
                    ),
                )
            ],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    f.foo(
                        column("partition", None, "_snuba_partition"),
                        alias="_snuba_exp",
                    ),
                ),
                SelectedExpression(
                    "exp",
                    f.foo(
                        column("partition", None, "_snuba_partition"),
                        alias="_snuba_exp",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    f.foo(
                        column("partition", None, "_snuba_partition"),
                        alias="_snuba_exp",
                    ),
                ),
                SelectedExpression(
                    "exp",
                    f.foo(
                        column("partition", None, "_snuba_partition"),
                        alias="_snuba_exp",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "exception_stacks.type",
                    column(
                        "exception_stacks.type", None, "_snuba_exception_stacks.type"
                    ),
                ),
            ],
            array_join=[column("exception_stacks.type", None, "exception_stacks.type")],
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                    f.like(column("exception_stacks.type"), literal("Arithmetic%")),
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "exception_stacks.type",
                    column(
                        "exception_stacks.type", None, "_snuba_exception_stacks.type"
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.arrayExists(
                        Lambda(
                            None,
                            ("x",),
                            f.assumeNotNull(
                                f.like(Argument(None, "x"), literal("Arithmetic%"))
                            ),
                        ),
                        column(
                            "exception_stacks.type",
                            None,
                            "_snuba_exception_stacks.type",
                        ),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                    f.like(column("exception_stacks.type"), literal("Arithmetic%")),
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "exception_stacks.type",
                    column(
                        "exception_stacks.type", None, "_snuba_exception_stacks.type"
                    ),
                ),
            ],
            array_join=[column("exception_stacks.type", None, "exception_stacks.type")],
            condition=and_cond(
                and_cond(
                    f.like(
                        column(
                            "exception_stacks.type",
                            None,
                            "_snuba_exception_stacks.type",
                        ),
                        literal("Arithmetic%"),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
            Query(
                from_clause=Entity(
                    EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
                ),
                selected_columns=[
                    SelectedExpression("count", f.count(alias="count")),
                    SelectedExpression(
                        "arrayJoin(exception_stacks)",
                        f.arrayJoin(
                            column("exception_stacks"),
                            alias="arrayJoin(exception_stacks)",
                        ),
                    ),
                ],
                array_join=None,
                condition=and_cond(
                    f.like(column("exception_stacks.type"), literal("Arithmetic%")),
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "arrayJoin(exception_stacks)",
                    f.arrayJoin(
                        column("exception_stacks", None, "_snuba_exception_stacks"),
                        alias="_snuba_arrayJoin(exception_stacks)",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.arrayExists(
                        Lambda(
                            None,
                            ("x",),
                            f.assumeNotNull(
                                f.like(Argument(None, "x"), literal("Arithmetic%"))
                            ),
                        ),
                        column(
                            "exception_stacks.type",
                            None,
                            "_snuba_exception_stacks.type",
                        ),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "exception_stacks.type",
                    column(
                        "exception_stacks.type", None, "_snuba_exception_stacks.type"
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.arrayExists(
                                Lambda(
                                    None,
                                    ("x",),
                                    f.assumeNotNull(
                                        f.equals(
                                            Argument(None, "x"),
                                            literal("ArithmeticException"),
                                        )
                                    ),
                                ),
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                            ),
                            f.arrayExists(
                                Lambda(
                                    None,
                                    ("x",),
                                    f.assumeNotNull(
                                        f.equals(
                                            Argument(None, "x"),
                                            literal("RuntimeException"),
                                        )
                                    ),
                                ),
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "arrayJoin(exception_stacks.type)",
                    f.arrayJoin(
                        column(
                            "exception_stacks.type",
                            None,
                            "_snuba_exception_stacks.type",
                        ),
                        alias="_snuba_arrayJoin(exception_stacks.type)",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.arrayExists(
                                Lambda(
                                    None,
                                    ("x",),
                                    f.assumeNotNull(
                                        f.equals(
                                            Argument(None, "x"),
                                            literal("ArithmeticException"),
                                        )
                                    ),
                                ),
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                            ),
                            f.arrayExists(
                                Lambda(
                                    None,
                                    ("x",),
                                    f.assumeNotNull(
                                        f.equals(
                                            Argument(None, "x"),
                                            literal("RuntimeException"),
                                        )
                                    ),
                                ),
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
                        ),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "tags_key", column("tags_key", None, "_snuba_tags_key")
                ),
                SelectedExpression("count", f.count(alias="_snuba_count")),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                f.ifNull(tags["foo"], literal("")), literal("baz")
                            ),
                            f.equals(
                                f.ifNull(tags["foo.bar"], literal("")), literal("qux")
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=[column("tags_key", None, "_snuba_tags_key")],
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
        (
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
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "exception_stacks.type",
                    column(
                        "exception_stacks.type", None, "_snuba_exception_stacks.type"
                    ),
                ),
            ],
            array_join=[column("exception_stacks", None, "exception_stacks")],
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                                literal("ArithmeticException"),
                            ),
                            f.equals(
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                                literal("RuntimeException"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp"),
                            f.toDateTime(literal("2021-01-01T00:00:00")),
                        ),
                        and_cond(
                            f.less(
                                column("timestamp"),
                                f.toDateTime(literal("2021-01-02T00:00:00")),
                            ),
                            f.equals(column("project_id"), literal(1)),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", f.count(alias="_snuba_count")),
                SelectedExpression(
                    "exception_stacks.type",
                    column(
                        "exception_stacks.type", None, "_snuba_exception_stacks.type"
                    ),
                ),
            ],
            array_join=[
                column("exception_stacks", None, "exception_stacks"),
            ],
            condition=and_cond(
                and_cond(
                    f.equals(
                        or_cond(
                            f.equals(
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                                literal("ArithmeticException"),
                            ),
                            f.equals(
                                column(
                                    "exception_stacks.type",
                                    None,
                                    "_snuba_exception_stacks.type",
                                ),
                                literal("RuntimeException"),
                            ),
                        ),
                        literal(1),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
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
        (
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
                        column("timestamp"),
                        f.toDateTime(literal("2021-01-01T00:00:00")),
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
            get_dataset("events"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id", column("group_id", None, "_snuba_group_id")
                ),
                SelectedExpression(
                    "group_id", column("group_id", None, "_snuba_group_id")
                ),
                SelectedExpression("count()", f.count(alias="_snuba_count()")),
                SelectedExpression(
                    "divide(uniq(tags[url]) AS a+*, 1)",
                    f.divide(
                        f.uniq(tags["url"], alias="_snuba_a+*"),
                        literal(1),
                        alias="_snuba_divide(uniq(tags[url]) AS a+*, 1)",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                    f.equals(
                        column("project_id", None, "_snuba_project_id"), literal(1)
                    ),
                ),
            ),
            groupby=[column("group_id", None, "_snuba_group_id")],
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
        (
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
                    or_cond(
                        literal(1), or_cond(literal(1), or_cond(literal(1), literal(1)))
                    ),
                    literal(0),
                ),
                order_by=None,
                limitby=None,
                limit=10,
                offset=0,
                totals=False,
                granularity=None,
            ),
            get_dataset("replays"),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.REPLAYS, get_entity(EntityKey.REPLAYS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "replay_id", column("replay_id", None, "_snuba_replay_id")
                ),
                SelectedExpression(
                    "replay_id", column("replay_id", None, "_snuba_replay_id")
                ),
            ],
            array_join=None,
            condition=and_cond(
                in_cond(
                    column("project_id", None, "_snuba_project_id"),
                    f.array(literal(4552673527463954)),
                ),
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2023, 9, 22, 18, 18, 10)),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2023, 6, 24, 18, 18, 10)),
                    ),
                ),
            ),
            groupby=[column("replay_id", None, "_snuba_replay_id")],
            having=f.notEquals(
                or_cond(
                    literal(1), or_cond(literal(1), or_cond(literal(1), literal(1)))
                ),
                literal(0),
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


@pytest.mark.parametrize("theinput, expected", test_cases)
def test_autogenerated(
    theinput: tuple[
        Query | CompositeQuery[LogicalDataSource],
        Dataset,
        QuerySettings | None,
        CustomProcessors | None,
    ],
    expected: Query | CompositeQuery[LogicalDataSource],
) -> None:
    query, dataset, settings, custom_processing = theinput
    timer = Timer("snql_pipeline")
    res = PostProcessAndValidateQuery().execute(
        QueryPipelineResult(
            data=(query, dataset, custom_processing),
            error=None,
            query_settings=settings or HTTPQuerySettings(),
            timer=timer,
        )
    )
    assert res.data and not res.error
    eq, reason = res.data.equals(expected)
    assert eq, reason


failure_cases = [
    pytest.param(
        (
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
            get_dataset("events"),
            None,
            None,
        ),
        AliasShadowingException,
    ),
    pytest.param(
        (
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
            get_dataset("events"),
            None,
            None,
        ),
        AliasShadowingException,
    ),
    pytest.param(
        (
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
            get_dataset("events"),
            None,
            None,
        ),
        CyclicAliasException,
    ),
    pytest.param(
        (
            Query(
                from_clause=Entity(
                    EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
                ),
                selected_columns=[
                    SelectedExpression(
                        "c", f.f1(f.f2(column("c"), alias="f2"), alias="c")
                    )
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
            get_dataset("events"),
            None,
            None,
        ),
        CyclicAliasException,
    ),
]


@pytest.mark.parametrize("theinput, expected_error", failure_cases)
def test_autogenerated_invalid(
    theinput: tuple[
        Query | CompositeQuery[LogicalDataSource],
        Dataset,
        QuerySettings | None,
        CustomProcessors | None,
    ],
    expected_error: Type[Exception],
) -> None:
    query, dataset, settings, custom_processing = theinput
    timer = Timer("snql_pipeline")
    res = PostProcessAndValidateQuery().execute(
        QueryPipelineResult(
            data=(query, dataset, custom_processing),
            error=None,
            query_settings=settings or HTTPQuerySettings(),
            timer=timer,
        )
    )
    assert res.error and not res.data
    assert isinstance(res.error, expected_error)
