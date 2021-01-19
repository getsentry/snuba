import threading
from typing import List, Tuple
from unittest.mock import Mock, call, ANY

from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.parser import parse_query
from snuba.request import Language, Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult


def test() -> None:
    cv = threading.Condition()
    query_result = QueryResult({}, {"stats": {}, "sql": ""})
    mock_query_runner = Mock(return_value=query_result)

    def callback_func(args: List[Tuple[str, QueryResult]]) -> None:
        with cv:
            cv.notify()

    mock_callback = Mock(side_effect=callback_func)

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
        callback_func=mock_callback,
    )

    with cv:
        delegator.build_execution_pipeline(
            Request(
                "", query_body, query, HTTPRequestSettings(), {}, "ref", Language.LEGACY
            ),
            mock_query_runner,
        ).execute()
        cv.wait(timeout=5)

    assert mock_query_runner.call_count == 2

    assert mock_callback.call_args == call(
        query,
        "ref",
        [Result("events", query_result, ANY), Result("errors", query_result, ANY)],
    )
