import threading
from typing import List, MutableSequence, Optional, Tuple, Union
from unittest.mock import ANY, Mock, call

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.pipeline_delegator import (
    PipelineDelegator,
    _is_query_copying_disallowed,
)
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.state import delete_config, set_config
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_query_copying_disallowed() -> None:
    set_config(
        "pipeline-delegator-disallow-query-copy", "subscription;subscriptions_executor"
    )

    assert _is_query_copying_disallowed("subscription") is True
    assert _is_query_copying_disallowed("tsdb-modelid:4") is False
    assert _is_query_copying_disallowed("subscriptions_executor") is True

    delete_config("pipeline-delegator-disallow-query-copy")


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
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
        query_plan_builder=StorageQueryPlanBuilder(
            storages=[
                EntityStorageConnection(
                    get_storage(StorageKey.ERRORS), TranslationMappers(), True
                )
            ],
            selector=DefaultQueryStorageSelector(),
        )
    )

    errors_ro_pipeline = SimplePipelineBuilder(
        query_plan_builder=StorageQueryPlanBuilder(
            storages=[
                EntityStorageConnection(
                    get_storage(StorageKey.ERRORS_RO), TranslationMappers()
                )
            ],
            selector=DefaultQueryStorageSelector(),
        ),
    )

    delegator = PipelineDelegator(
        query_pipeline_builders={
            "errors": errors_pipeline,
            "errors_ro": errors_ro_pipeline,
        },
        selector_func=lambda query, referrer: ("errors", ["errors_ro"]),
        split_rate_limiter=True,
        ignore_secondary_exceptions=True,
        callback_func=mock_callback,
    )

    runner_call_count = 0
    runner_settings: MutableSequence[QuerySettings] = []

    def query_runner(
        clickhouse_query: Union[Query, CompositeQuery[Table]],
        query_settings: QuerySettings,
        reader: Reader,
    ) -> QueryResult:
        nonlocal runner_call_count
        nonlocal runner_settings

        runner_call_count += 1
        runner_settings.append(query_settings)
        return query_result

    set_config("pipeline_split_rate_limiter", 1)

    with cv:
        query_settings = HTTPQuerySettings(referrer="ref")
        delegator.build_execution_pipeline(
            Request(
                id="asd",
                original_body=query_body,
                query=query,
                snql_anonymized="",
                query_settings=query_settings,
                attribution_info=AttributionInfo(
                    get_app_id("ref"),
                    {"tenant_type": "tenant_id"},
                    "ref",
                    None,
                    None,
                    None,
                ),
            ),
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
        query_settings,
        "ref",
        Result("errors", query_result, ANY),
        [Result("errors_ro", query_result, ANY)],
    )
