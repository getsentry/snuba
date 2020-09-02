from typing import Sequence

import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.errors_replacer import (
    ReplacerState,
    set_project_exclude_groups,
    set_project_needs_final,
)
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.query.conditions import BooleanFunctions
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.redis import redis_client
from snuba.request.request_settings import HTTPRequestSettings


def build_in(project_column: str, projects: Sequence[int]) -> Expression:
    return FunctionCall(
        None,
        "in",
        (
            Column(None, None, project_column),
            FunctionCall(None, "tuple", tuple([Literal(None, p) for p in projects])),
        ),
    )


@pytest.fixture
def query() -> ClickhouseQuery:
    return ClickhouseQuery(
        LogicalQuery(
            {"conditions": [("project_id", "IN", [2])]},
            TableSource("my_table", ColumnSet([])),
            condition=build_in("project_id", [2]),
        )
    )


def teardown_function() -> None:
    redis_client.flushdb()


def test_with_turbo(query: ClickhouseQuery) -> None:
    PostReplacementConsistencyEnforcer("project_id", None).process_query(
        query, HTTPRequestSettings(turbo=True)
    )

    assert query.get_conditions() == [("project_id", "IN", [2])]
    assert query.get_condition_from_ast() == build_in("project_id", [2])


def test_without_turbo_with_projects_needing_final(query: ClickhouseQuery) -> None:
    set_project_needs_final(2, ReplacerState.EVENTS)

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.EVENTS
    ).process_query(query, HTTPRequestSettings())

    assert query.get_conditions() == [("project_id", "IN", [2])]
    assert query.get_condition_from_ast() == build_in("project_id", [2])
    assert query.get_final()


def test_without_turbo_without_projects_needing_final(query: ClickhouseQuery) -> None:
    PostReplacementConsistencyEnforcer("project_id", None).process_query(
        query, HTTPRequestSettings()
    )

    assert query.get_conditions() == [("project_id", "IN", [2])]
    assert query.get_condition_from_ast() == build_in("project_id", [2])
    assert not query.get_final()


def test_not_many_groups_to_exclude(query: ClickhouseQuery) -> None:
    state.set_config("max_group_ids_exclude", 5)
    set_project_exclude_groups(2, [100, 101, 102], ReplacerState.EVENTS)

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.EVENTS
    ).process_query(query, HTTPRequestSettings())

    expected = [
        ("project_id", "IN", [2]),
        (["assumeNotNull", ["group_id"]], "NOT IN", [100, 101, 102]),
    ]
    assert query.get_conditions() == expected
    assert query.get_condition_from_ast() == FunctionCall(
        None,
        BooleanFunctions.AND,
        (
            FunctionCall(
                None,
                "notIn",
                (
                    FunctionCall(
                        None, "assumeNotNull", (Column(None, None, "group_id"),)
                    ),
                    FunctionCall(
                        None,
                        "tuple",
                        (Literal(None, 100), Literal(None, 101), Literal(None, 102),),
                    ),
                ),
            ),
            build_in("project_id", [2]),
        ),
    )
    assert not query.get_final()


def test_too_many_groups_to_exclude(query: ClickhouseQuery) -> None:
    state.set_config("max_group_ids_exclude", 2)
    set_project_exclude_groups(2, [100, 101, 102], ReplacerState.EVENTS)

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.EVENTS
    ).process_query(query, HTTPRequestSettings())

    assert query.get_conditions() == [("project_id", "IN", [2])]
    assert query.get_condition_from_ast() == build_in("project_id", [2])
    assert query.get_final()
