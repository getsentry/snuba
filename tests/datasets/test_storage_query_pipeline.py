from datetime import datetime
from functools import partial

import pytest

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.plans.storage_plan_builder import StorageOnlyQueryPlanBuilder
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_for_storage
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer
from snuba.web.query import _run_and_apply_column_names


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_parse_snql_for_storage():
    # starter info
    query_str = f"""MATCH (metrics_summaries)
                    SELECT groupUniqArray(span_id) AS unique_span_ids BY project_id, metric_mri
                    WHERE project_id = 1
                    AND metric_mri = 'abcd'
                    AND end_timestamp >= toDateTime('{datetime.now().isoformat()}')
                    AND end_timestamp < toDateTime('{datetime.now().isoformat()}')
                    GRANULARITY 60
                    """
    storage = get_storage(StorageKey("metrics_summaries"))
    settings = HTTPQuerySettings()

    atinfo = AttributionInfo(
        app_id=AppID("kylesapp"),
        tenant_ids={"referrer": "tests", "organization_id": 1},
        referrer="cat",
        team=None,
        feature=None,
        parent_api=None,
    )
    # snmeta = SnubaQueryMetadata(request=None, dataset="obama", timer=None)
    query_runner = partial(
        _run_and_apply_column_names,
        timer=Timer("test-storage"),
        query_metadata=None,
        attribution_info=atinfo,
        robust=False,
        concurrent_queries_gauge=None,
    )

    # parse snql -> storage
    query = parse_snql_for_storage(query_str, settings=settings)

    # set the from clause
    relational_source = storage.get_schema().get_data_source()
    assert isinstance(relational_source, TableSource)
    query.set_from_clause(
        Table(
            table_name=relational_source.get_table_name(),
            schema=relational_source.get_columns(),
            allocation_policies=storage.get_allocation_policies(),
            mandatory_conditions=relational_source.get_mandatory_conditions(),
        )
    )

    # physical query -> query plan
    # execute
    res = (
        StorageOnlyQueryPlanBuilder()
        .build_and_rank_plans(query, storage, HTTPQuerySettings())[0]
        .execution_strategy.execute(query, settings, query_runner)
    )
    print(res)
