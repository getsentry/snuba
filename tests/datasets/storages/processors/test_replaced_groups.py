from datetime import datetime, timedelta
from typing import Sequence

import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.processor import ReplacementType
from snuba.query.conditions import BooleanFunctions
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.query.query_settings import HTTPQuerySettings, SubscriptionQuerySettings
from snuba.redis import RedisClientKey, get_redis_client
from snuba.replacers.projects_query_flags import ProjectsQueryFlags
from snuba.replacers.replacer_processor import ReplacerState


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
            None,
            "less",
            (Column(None, None, "timestamp"), Literal(None, query_to)),
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
        Table("my_table", ColumnSet([])),
        condition=build_in("project_id", [2]),
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
        Table("my_table", ColumnSet([])),
        condition=build_group_id_condition(),
    )


@pytest.fixture
def query_with_multiple_group_ids() -> ClickhouseQuery:
    return ClickhouseQuery(
        Table("my_table", ColumnSet([])),
        condition=build_group_ids_condition(),
    )


def teardown_function() -> None:
    get_redis_client(RedisClientKey.REPLACEMENTS_STORE).flushdb()


@pytest.mark.redis_db
def test_with_turbo(query: ClickhouseQuery) -> None:
    PostReplacementConsistencyEnforcer("project_id", None).process_query(
        query, HTTPQuerySettings(turbo=True)
    )

    assert query.get_condition() == build_in("project_id", [2])


@pytest.mark.redis_db
def test_without_turbo_with_projects_needing_final(query: ClickhouseQuery) -> None:
    ProjectsQueryFlags.set_project_needs_final(
        2,
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.ERRORS
    ).process_query(query, HTTPQuerySettings())

    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final


@pytest.mark.redis_db
def test_without_turbo_without_projects_needing_final(query: ClickhouseQuery) -> None:
    PostReplacementConsistencyEnforcer("project_id", None).process_query(
        query, HTTPQuerySettings()
    )

    assert query.get_condition() == build_in("project_id", [2])
    assert not query.get_from_clause().final


@pytest.mark.redis_db
def test_remove_final_subscriptions(query: ClickhouseQuery) -> None:
    ProjectsQueryFlags.set_project_needs_final(
        2,
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.ERRORS
    ).process_query(query, SubscriptionQuerySettings())
    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final

    state.set_config("skip_final_subscriptions_projects", "[2,3,4]")
    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.ERRORS
    ).process_query(query, SubscriptionQuerySettings())
    assert not query.get_from_clause().final


@pytest.mark.redis_db
def test_not_many_groups_to_exclude(query: ClickhouseQuery) -> None:
    state.set_config("max_group_ids_exclude", 5)
    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.ERRORS
    ).process_query(query, HTTPQuerySettings())

    assert query.get_condition() == build_and(
        FunctionCall(
            None,
            "notIn",
            (
                FunctionCall(None, "assumeNotNull", (Column(None, None, "group_id"),)),
                FunctionCall(
                    None,
                    "tuple",
                    (
                        Literal(None, 100),
                        Literal(None, 101),
                        Literal(None, 102),
                    ),
                ),
            ),
        ),
        build_in("project_id", [2]),
    )
    assert not query.get_from_clause().final


@pytest.mark.redis_db
def test_too_many_groups_to_exclude(query: ClickhouseQuery) -> None:
    state.set_config("max_group_ids_exclude", 2)
    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    PostReplacementConsistencyEnforcer(
        "project_id", ReplacerState.ERRORS
    ).process_query(query, HTTPQuerySettings())

    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final


@pytest.mark.redis_db
def test_query_overlaps_replacements_processor(
    query: ClickhouseQuery,
    query_with_timestamp: ClickhouseQuery,
    query_with_future_timestamp: ClickhouseQuery,
) -> None:
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    # replacement time unknown, default to "overlaps" but no groups to exclude so shouldn't be final
    enforcer._set_query_final(query_with_timestamp, True)
    enforcer.process_query(query_with_timestamp, HTTPQuerySettings())
    assert not query_with_timestamp.get_from_clause().final

    # overlaps replacement and should be final due to too many groups to exclude
    state.set_config("max_group_ids_exclude", 2)
    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )
    enforcer._set_query_final(query_with_timestamp, False)
    enforcer.process_query(query_with_timestamp, HTTPQuerySettings())
    assert query_with_timestamp.get_from_clause().final

    # query time range unknown and should be final due to too many groups to exclude
    enforcer._set_query_final(query, False)
    enforcer.process_query(query, HTTPQuerySettings())
    assert query.get_from_clause().final

    # doesn't overlap replacements
    enforcer._set_query_final(query_with_future_timestamp, True)
    enforcer.process_query(query_with_future_timestamp, HTTPQuerySettings())
    assert not query_with_future_timestamp.get_from_clause().final


@pytest.mark.redis_db
def test_single_no_replacements(query_with_single_group_id: ClickhouseQuery) -> None:
    """
    Query is looking for a group that has not been replaced, but the project itself
    has replacements.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [105, 106, 107],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_single_group_id, True)
    state.set_config("max_group_ids_exclude", 2)

    enforcer.process_query(query_with_single_group_id, HTTPQuerySettings())
    assert query_with_single_group_id.get_condition() == build_and(
        build_in("project_id", [2]), build_in("group_id", [101])
    )
    assert not query_with_single_group_id.get_from_clause().final


@pytest.mark.redis_db
def test_single_too_many_exclude(query_with_single_group_id: ClickhouseQuery) -> None:
    """
    Query is looking for a group that has been replaced, and there are too many
    groups to exclude.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_single_group_id, True)
    state.set_config("max_group_ids_exclude", 2)

    enforcer.process_query(query_with_single_group_id, HTTPQuerySettings())
    assert query_with_single_group_id.get_condition() == build_and(
        build_not_in("group_id", [101]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101])),
    )
    assert not query_with_single_group_id.get_from_clause().final


@pytest.mark.redis_db
def test_single_not_too_many_exclude(
    query_with_single_group_id: ClickhouseQuery,
) -> None:
    """
    Query is looking for a group that has been replaced, and there are not too many
    groups to exclude.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_single_group_id, True)
    state.set_config("max_group_ids_exclude", 5)

    enforcer.process_query(query_with_single_group_id, HTTPQuerySettings())
    assert query_with_single_group_id.get_condition() == build_and(
        build_not_in("group_id", [101]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101])),
    )
    assert not query_with_single_group_id.get_from_clause().final


@pytest.mark.redis_db
def test_multiple_disjoint_replaced(
    query_with_multiple_group_ids: ClickhouseQuery,
) -> None:
    """
    Query is looking for multiple groups and there are replaced groups, but these
    sets of group ids are disjoint. (No queried groups have been replaced)
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [110, 120, 130],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_multiple_group_ids, True)
    state.set_config("max_group_ids_exclude", 5)

    enforcer.process_query(query_with_multiple_group_ids, HTTPQuerySettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_in("project_id", [2]), build_in("group_id", [101, 102])
    )
    assert not query_with_multiple_group_ids.get_from_clause().final


@pytest.mark.redis_db
def test_multiple_fewer_exclude_than_queried(
    query_with_multiple_group_ids: ClickhouseQuery,
) -> None:
    """
    Query is looking for multiple groups and there are replaced groups, but there
    are fewer excluded groups than queried groups.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [101],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_multiple_group_ids, True)
    state.set_config("max_group_ids_exclude", 5)

    enforcer.process_query(query_with_multiple_group_ids, HTTPQuerySettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_not_in("group_id", [101]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101, 102])),
    )
    assert not query_with_multiple_group_ids.get_from_clause().final


@pytest.mark.redis_db
def test_multiple_too_many_excludes(
    query_with_multiple_group_ids: ClickhouseQuery,
) -> None:
    """
    Query is looking for multiple groups and there are too many groups to exclude, but
    there are fewer groups queried for than replaced.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_multiple_group_ids, True)
    state.set_config("max_group_ids_exclude", 2)

    enforcer.process_query(query_with_multiple_group_ids, HTTPQuerySettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_not_in("group_id", [101, 102]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101, 102])),
    )

    assert not query_with_multiple_group_ids.get_from_clause().final


@pytest.mark.redis_db
def test_multiple_not_too_many_excludes(
    query_with_multiple_group_ids: ClickhouseQuery,
) -> None:
    """
    Query is looking for multiple groups and there are not too many groups to exclude, but
    there are fewer groups queried for than replaced.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query_with_multiple_group_ids, True)
    state.set_config("max_group_ids_exclude", 5)

    enforcer.process_query(query_with_multiple_group_ids, HTTPQuerySettings())
    assert query_with_multiple_group_ids.get_condition() == build_and(
        build_not_in("group_id", [101, 102]),
        build_and(build_in("project_id", [2]), build_in("group_id", [101, 102])),
    )
    assert not query_with_multiple_group_ids.get_from_clause().final


@pytest.mark.redis_db
def test_no_groups_not_too_many_excludes(query: ClickhouseQuery) -> None:
    """
    Query has no groups, and not too many to exclude.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query, True)
    state.set_config("max_group_ids_exclude", 3)

    enforcer.process_query(query, HTTPQuerySettings())
    assert query.get_condition() == build_and(
        build_not_in("group_id", [100, 101, 102]),
        build_in("project_id", [2]),
    )
    assert not query.get_from_clause().final


@pytest.mark.redis_db
def test_no_groups_too_many_excludes(query: ClickhouseQuery) -> None:
    """
    Query has no groups, and too many to exclude.
    """
    enforcer = PostReplacementConsistencyEnforcer("project_id", ReplacerState.ERRORS)

    ProjectsQueryFlags.set_project_exclude_groups(
        2,
        [100, 101, 102],
        ReplacerState.ERRORS,
        ReplacementType.EXCLUDE_GROUPS,  # Arbitrary replacement type, no impact on tests
    )

    enforcer._set_query_final(query, True)
    state.set_config("max_group_ids_exclude", 1)

    enforcer.process_query(query, HTTPQuerySettings())
    assert query.get_condition() == build_in("project_id", [2])
    assert query.get_from_clause().final
