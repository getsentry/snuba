from datetime import datetime, timedelta
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
from snuba.datasets.events_processor_base import ReplacementType
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.query.conditions import BooleanFunctions
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.redis import redis_client
from snuba.request.request_settings import HTTPRequestSettings


def build_in(column: str, items: Sequence[int]) -> Expression:
    return FunctionCall(
        None,
        "in",
        (
            Column(None, None, column),
            FunctionCall(None, "tuple", tuple([Literal(None, p) for p in items])),
        ),
    )


def build_not_in(column: str, items: Sequence[int]) -> Expression:
    return FunctionCall(
        None,
        "notIn",
        (
            FunctionCall(None, "assumeNotNull", (Column(None, None, column),)),
            FunctionCall(None, "tuple", tuple([Literal(None, p) for p in items])),
        ),
    )


def build_time_range(query_from: datetime, query_to: datetime) -> Expression:
    return build_and(
        FunctionCall(
            None,
            "greaterOrEquals",
            (Column(None, None, "timestamp"), Literal(None, query_from)),
        ),
        FunctionCall(
            None, "less", (Column(None, None, "timestamp"), Literal(None, query_to)),
        ),
    )


def build_and(expression_one: Expression, expression_two: Expression) -> Expression:
    return FunctionCall(None, BooleanFunctions.AND, (expression_one, expression_two))


def build_group_id_condition() -> Expression:
    return build_and(build_in("project_id", [2]), build_in("group_id", [101]))


def build_group_ids_condition() -> Expression:
    return build_and(build_in("project_id", [2]), build_in("group_id", [101, 102]))


@pytest.fixture
def query() -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("my_table", ColumnSet([])), condition=build_in("project_id", [2]),
    )


@pytest.fixture
def query_with_timestamp() -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("my_table", ColumnSet([])),
        condition=build_and(
            build_in("project_id", [2]),
            build_time_range(datetime(2021, 1, 1), datetime(2021, 1, 2)),
        ),
    )


@pytest.fixture
def query_with_future_timestamp() -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("my_table", ColumnSet([])),
        condition=build_and(
            build_in("project_id", [2]),
            build_time_range(
                datetime.now() + timedelta(days=1), datetime.now() + timedelta(days=2)
            ),
        ),
    )


@pytest.fixture
def query_with_single_group_id() -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("my_table", ColumnSet([])), condition=build_group_id_condition(),
    )


@pytest.fixture
def query_with_multiple_group_ids() -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("my_table", ColumnSet([])), condition=build_group_ids_condition(),
    )


def teardown_function() -> None:
    redis_client.flushdb()


def test_with_turbo(query: ClickhouseQuery) -> None:
    PostReplacementConsistencyEnforcer("project_id", None).process_query(
        query, HTTPRequestSettings(turbo=True)
    )

    assert query.get_condition() == build_in("project_id", [2])


def test_without_turbo_with_projects_needing_final(query: ClickhouseQuery) -> None:
    set_project_needs_final(
        2,
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.EVENTS
    ).process_query(query, HTTPRequestSettings())

    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final


def test_without_turbo_without_projects_needing_final(query: ClickhouseQuery) -> None:
    PostReplacementConsistencyEnforcer("project_id", None).process_query(
        query, HTTPRequestSettings()
    )

    assert query.get_condition() == build_in("project_id", [2])
    assert not query.get_from_clause().final


def test_not_many_groups_to_exclude(query: ClickhouseQuery) -> None:
    state.set_config("max_group_ids_exclude", 5)
    set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.EVENTS
    ).process_query(query, HTTPRequestSettings())

    assert query.get_condition() == build_and(
        FunctionCall(
            None,
            "notIn",
            (
                FunctionCall(None, "assumeNotNull", (Column(None, None, "group_id"),)),
                FunctionCall(
                    None,
                    "tuple",
                    (Literal(None, 100), Literal(None, 101), Literal(None, 102),),
                ),
            ),
        ),
        build_in("project_id", [2]),
    )
    assert not query.get_from_clause().final


def test_too_many_groups_to_exclude(query: ClickhouseQuery) -> None:
    state.set_config("max_group_ids_exclude", 2)
    set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.EVENTS
    ).process_query(query, HTTPRequestSettings())

    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final


def test_query_overlaps_replacements_processor(
    query: ClickhouseQuery,
    query_with_timestamp: ClickhouseQuery,
    query_with_future_timestamp: ClickhouseQuery,
) -> None:
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.EVENTS)

    # replacement time unknown, default to "overlaps" but no groups to exclude so shouldn't be final
    enforcer._set_query_final(query_with_timestamp, True)
    enforcer.process_query(query_with_timestamp, HTTPRequestSettings())
    assert not query_with_timestamp.get_from_clause().final

    # overlaps replacement and should be final due to too many groups to exclude
    state.set_config("max_group_ids_exclude", 2)
    set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )
    enforcer._set_query_final(query_with_timestamp, False)
    enforcer.process_query(query_with_timestamp, HTTPRequestSettings())
    assert query_with_timestamp.get_from_clause().final

    # query time range unknown and should be final due to too many groups to exclude
    enforcer._set_query_final(query, False)
    enforcer.process_query(query, HTTPRequestSettings())
    assert query.get_from_clause().final

    # doesn't overlap replacements
    enforcer._set_query_final(query_with_future_timestamp, True)
    enforcer.process_query(query_with_future_timestamp, HTTPRequestSettings())
    assert not query_with_future_timestamp.get_from_clause().final


def test_query_has_group_id(query_with_single_group_id: ClickhouseQuery,) -> None:
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.EVENTS)

    set_project_exclude_groups(
        2,
        [105, 106, 107],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    # Query looking for groups with no replacements, but project has replacements
    enforcer._set_query_final(query_with_single_group_id, True)
    state.set_config("max_group_ids_exclude", 2)

    enforcer.process_query(query_with_single_group_id, HTTPRequestSettings())
    assert query_with_single_group_id.get_condition() == build_and(
        build_in("project_id", [2]), build_in("group_id", [101])
    )
    assert not query_with_single_group_id.get_from_clause().final

    set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    # Too many groups to exclude, but query only needs one of them
    enforcer._set_query_final(query_with_single_group_id, True)
    state.set_config("max_group_ids_exclude", 2)

    enforcer.process_query(query_with_single_group_id, HTTPRequestSettings())
    assert query_with_single_group_id.get_condition() == build_and(
        build_not_in("group_id", [101]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101])),
    )
    assert not query_with_single_group_id.get_from_clause().final

    # Not too many groups to exclude, but query only needs one of them
    query_with_single_group_id.set_ast_condition(build_group_id_condition())
    enforcer._set_query_final(query_with_single_group_id, True)
    state.set_config("max_group_ids_exclude", 5)

    enforcer.process_query(query_with_single_group_id, HTTPRequestSettings())
    assert query_with_single_group_id.get_condition() == build_and(
        build_not_in("group_id", [101]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101])),
    )
    assert not query_with_single_group_id.get_from_clause().final


def test_query_has_group_ids(query_with_multiple_group_ids: ClickhouseQuery,) -> None:
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.EVENTS)

    set_project_exclude_groups(
        2,
        [101],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    # Fewer groups to exclude than query is looking for
    enforcer._set_query_final(query_with_multiple_group_ids, True)

    state.set_config("max_group_ids_exclude", 5)
    enforcer.process_query(query_with_multiple_group_ids, HTTPRequestSettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_not_in("group_id", [101]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101, 102])),
    )
    assert not query_with_multiple_group_ids.get_from_clause().final

    set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    # Too many groups to exclude, but query only needs a few of them
    query_with_multiple_group_ids.set_ast_condition(build_group_ids_condition())

    enforcer._set_query_final(query_with_multiple_group_ids, True)
    state.set_config("max_group_ids_exclude", 2)
    enforcer.process_query(query_with_multiple_group_ids, HTTPRequestSettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_not_in("group_id", [101, 102]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101, 102])),
    )

    assert not query_with_multiple_group_ids.get_from_clause().final

    # Not too many groups to exclude, but query only needs a few of them
    query_with_multiple_group_ids.set_ast_condition(build_group_ids_condition())
    enforcer._set_query_final(query_with_multiple_group_ids, True)

    state.set_config("max_group_ids_exclude", 5)
    enforcer.process_query(query_with_multiple_group_ids, HTTPRequestSettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_not_in("group_id", [101, 102]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101, 102])),
    )
    assert not query_with_multiple_group_ids.get_from_clause().final


def test_query_without_group_ids(query: ClickhouseQuery) -> None:
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.EVENTS)

    set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.EVENTS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    # Too many groups to exclude, but query has no group_id condition
    enforcer._set_query_final(query, True)
    state.set_config("max_group_ids_exclude", 1)

    enforcer.process_query(query, HTTPRequestSettings())
    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final

    # Not too many groups to exclude, and query has none of them
    query.set_ast_condition(build_in("project_id", [2]))
    enforcer._set_query_final(query, True)
    state.set_config("max_group_ids_exclude", 3)

    enforcer.process_query(query, HTTPRequestSettings())
    assert query.get_condition() == build_and(
        build_not_in("group_id", [100, 101, 102]), build_in("project_id", [2]),
    )
    assert not query.get_from_clause().final
