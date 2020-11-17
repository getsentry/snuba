import time
from unittest.mock import Mock, call

from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
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

    events_pipeline = SimplePipelineBuilder(
        query_plan_builder=SingleStorageQueryPlanBuilder(
            storage=get_storage(StorageKey.EVENTS)
        ),
    )

    errors_pipeline = SimplePipelineBuilder(
        query_plan_builder=SingleStorageQueryPlanBuilder(
            storage=get_storage(StorageKey.ERRORS)
        ),
    )

    delegator = PipelineDelegator(
        query_pipeline_builders={"events": events_pipeline, "errors": errors_pipeline},
        selector_func=lambda query: ("events", ["errors"]),
        callback_func=mock_callback_func,
    )

    delegator.build_pipeline(
        Request("", query, HTTPRequestSettings(), {}, ""), mock_query_runner
    ).execute()

    # Allow threads to finish
    time.sleep(0.01)

    assert mock_query_runner.call_count == 2

    assert mock_callback_func.call_args_list == [
        call([("events", query_result), ("errors", query_result)])
    ]
