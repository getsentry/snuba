from typing import List, Optional

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.events import errors_translators
from snuba.datasets.entities.storage_selectors.errors import ErrorsQueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
    QueryStorageSelector,
)
from snuba.datasets.entities.transactions import transaction_translator
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import StorageAndMappers
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Request

TEST_CASES = [
    pytest.param(
        """
        MATCH (transactions)
        SELECT col1
        WHERE tags_key IN tuple('t1', 't2')
            AND finish_ts >= toDateTime('2021-01-01T00:00:00')
            AND finish_ts < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        get_dataset("transactions"),
        [
            StorageAndMappers(
                get_storage(StorageKey.TRANSACTIONS), transaction_translator
            ),
        ],
        DefaultQueryStorageSelector(),
        None,
        StorageSetKey.TRANSACTIONS,
        id="Single storage",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT col1
        WHERE tags_key IN tuple('t1', 't2')
            AND timestamp >= toDateTime('2021-01-01T00:00:00')
            AND timestamp < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        get_dataset("events"),
        [
            StorageAndMappers(get_storage(StorageKey.ERRORS_RO), errors_translators),
            StorageAndMappers(
                get_writable_storage(StorageKey.ERRORS), errors_translators
            ),
        ],
        ErrorsQueryStorageSelector(),
        None,
        StorageSetKey.EVENTS,
        id="Multiple storages and selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_and_mappers, selector, partition_key_column_name, expected_storage_set_key",
    TEST_CASES,
)
def test_storage_query_plan_builder(
    snql_query: str,
    dataset: Dataset,
    storage_and_mappers: List[StorageAndMappers],
    selector: Optional[QueryStorageSelector],
    partition_key_column_name: Optional[str],
    expected_storage_set_key: StorageKey,
) -> None:
    query_plan_builder = StorageQueryPlanBuilder(
        storages=storage_and_mappers,
        selector=selector,
        partition_key_column_name=partition_key_column_name,
    )
    query, snql_anonymized = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)
    request = Request(
        id="a",
        original_body={"query": snql_query, "dataset": "transactions"},
        query=query,
        snql_anonymized=snql_anonymized,
        query_settings=HTTPQuerySettings(referrer="r"),
        attribution_info=AttributionInfo(get_app_id("blah"), "blah", None, None, None),
    )
    plan: ClickhouseQueryPlan = query_plan_builder.build_and_rank_plans(
        query=query, settings=request.query_settings
    )[0]

    assert plan.storage_set_key == expected_storage_set_key
