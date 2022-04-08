import threading
from typing import List, MutableSequence, Optional, Tuple, Union
from unittest.mock import ANY, Mock, call

from snuba.attribution import get_app_id
from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.state import set_config
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult


def test() -> None:
    cv = threading.Condition()
    query_result = QueryResult({}, {"stats": {}, "sql": "", "experiments": {}})

    def callback_func(
        primary: Optional[Tuple[str, QueryResult]], other: List[Tuple[str, QueryResult]]
    ) -> None:
        with cv:
            cv.notify()

    mock_callback = Mock(side_effect=callback_func)

    query_body = {
        "query": """
        MATCH (events)
        SELECT type, project_id
        WHERE project_id = 1
        AND timestamp >= toDateTime('2020-01-01 12:00:00')
        AND timestamp < toDateTime('2020-01-02 12:00:00')
        """,
        "dataset": "events",
    }

    events = get_dataset("events")
    query, _ = parse_snql_query(query_body["query"], events)

    errors_pipeline = SimplePipelineBuilder(
        query_plan_builder=SingleStorageQueryPlanBuilder(
            storage=get_storage(StorageKey.ERRORS)
        ),
    )

    errors_ro_pipeline = SimplePipelineBuilder(
        query_plan_builder=SingleStorageQueryPlanBuilder(
            storage=get_storage(StorageKey.ERRORS_RO)
        ),
    )

    delegator = PipelineDelegator(
        query_pipeline_builders={
            "errors": errors_pipeline,
            "errors_ro": errors_ro_pipeline,
        },
        selector_func=lambda query, referrer: ("errors", ["errors_ro"]),
        split_rate_limiter=True,
        callback_func=mock_callback,
    )

    runner_call_count = 0
    runner_settings: MutableSequence[RequestSettings] = []

    def query_runner(
        query: Union[Query, CompositeQuery[Table]],
        settings: RequestSettings,
        reader: Reader,
    ) -> QueryResult:
        nonlocal runner_call_count
        nonlocal runner_settings

        runner_call_count += 1
        runner_settings.append(settings)
        return query_result

    set_config("pipeline_split_rate_limiter", 1)

    with cv:
        request_settings = HTTPRequestSettings(referrer="ref")
        delegator.build_execution_pipeline(
            Request("", query_body, query, get_app_id("default"), "", request_settings),
            query_runner,
        ).execute()
        cv.wait(timeout=5)

    assert runner_call_count == 2
    assert len(runner_settings) == 2
    settings, settings_ro = runner_settings
    # Validate that settings have been duplicated
    assert id(settings) != id(settings_ro)

    assert mock_callback.call_args == call(
        query,
        request_settings,
        "ref",
        Result("errors", query_result, ANY),
        [Result("errors_ro", query_result, ANY)],
    )
