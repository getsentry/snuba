from unittest.mock import Mock, call

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.query_plan_delegator import QueryPlanDelegator
from snuba.query.parser import parse_query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.web import QueryResult


def test() -> None:
    query_result = QueryResult({}, {"stats": {}, "sql": ""})
    mock_query_runner = Mock(return_value=query_result)
    mock_callback_func = Mock()
    query_body = {
        "selected_columns": ["type", "project_id"],
    }

    events = get_dataset("events")
    query = parse_query(query_body, events)

    events_query_plan_builder = SingleStorageQueryPlanBuilder(
        storage=get_storage(StorageKey.EVENTS)
    )
    events_ro_query_plan_builder = SingleStorageQueryPlanBuilder(
        storage=get_storage(StorageKey.EVENTS_RO)
    )

    delegator = QueryPlanDelegator(
        query_plan_builders={
            "events": events_query_plan_builder,
            "events_ro": events_ro_query_plan_builder,
        },
        selector_func=lambda query: ["events", "events_ro"],
        callback_func=mock_callback_func,
    )

    query_plan = delegator.build_plan(Request("", query, HTTPRequestSettings(), {}, ""))

    clickhouse_query = ClickhouseQuery(
        events.get_default_entity()
        .get_all_storages()[0]
        .get_schema()
        .get_data_source(),
    )

    query_plan.execution_strategy.execute(
        clickhouse_query, HTTPRequestSettings(), mock_query_runner
    )

    assert mock_query_runner.call_count == 2

    assert mock_callback_func.call_args_list == [
        call([("events", query_result), ("events_ro", query_result)])
    ]
