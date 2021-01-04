from typing import Mapping, Sequence

import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.single_storage import SimpleQueryPlanExecutionStrategy
from snuba.pipeline.plans_selector import select_best_plans
from snuba.query.data_source.simple import Table


def build_plan(table_name: str, storage_set: StorageSetKey) -> ClickhouseQueryPlan:
    return ClickhouseQueryPlan(
        Query(Table(table_name, ColumnSet([]))),
        SimpleQueryPlanExecutionStrategy(
            get_cluster(storage_set), db_query_processors=[],
        ),
        storage_set,
        plan_query_processors=[],
        db_query_processors=[],
    )


TEST_CASES = [
    pytest.param(
        {
            "events": [build_plan("events_table", StorageSetKey.EVENTS)],
            "groups": [build_plan("groups_table", StorageSetKey.EVENTS)],
        },
        {"events": "events_table", "groups": "groups_table"},
        id="All tables on the same storage sets",
    ),
    pytest.param(
        {
            "events": [
                build_plan("events_readonly_table", StorageSetKey.EVENTS),
                build_plan("events_table", StorageSetKey.EVENTS),
            ],
            "groups": [build_plan("groups_table", StorageSetKey.EVENTS)],
        },
        {"events": "events_readonly_table", "groups": "groups_table"},
        id="Multiple plans for the same alias on the same storage set",
    ),
    pytest.param(
        {
            "events": [
                build_plan("events_readonly_table", StorageSetKey.EVENTS_RO),
                build_plan("events_table", StorageSetKey.EVENTS),
            ],
            "groups": [build_plan("groups_table", StorageSetKey.EVENTS)],
        },
        {"events": "events_table", "groups": "groups_table"},
        id="Highest ranking plan for events in the wrong storage set. Skip",
    ),
    pytest.param(
        {
            "events": [
                build_plan("events_readonly_table", StorageSetKey.EVENTS_RO),
                build_plan("events_table", StorageSetKey.EVENTS),
            ],
            "groups": [
                build_plan("groups_table", StorageSetKey.EVENTS),
                build_plan("another_group_table", StorageSetKey.EVENTS),
                build_plan("groups_readonly_table", StorageSetKey.EVENTS_RO),
            ],
        },
        {"events": "events_table", "groups": "groups_table"},
        id="Two valid storage sets, pick the highest ranking one",
    ),
]


@pytest.mark.parametrize("plan_ranks, expected_selected_tables", TEST_CASES)
def test_ranking(
    plan_ranks: Mapping[str, Sequence[ClickhouseQueryPlan]],
    expected_selected_tables: Mapping[str, str],
) -> None:
    selected_tables = {
        alias: plan.query.get_from_clause().table_name
        for alias, plan in select_best_plans(plan_ranks).items()
    }
    assert selected_tables == expected_selected_tables
