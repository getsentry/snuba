from datetime import datetime, timedelta
from functools import partial
from typing import Mapping, MutableMapping

import pytest
import pytz

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.generic_metrics import GenericMetricsSetsEntity
from snuba.datasets.entities.metrics import TagsTypeTransformer
from snuba.datasets.factory import get_dataset
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import Query
from snuba.query.processors.logical.granularity_processor import (
    DEFAULT_MAPPED_GRANULARITY_ENUM,
    PERFORMANCE_GRANULARITIES,
    MappedGranularityProcessor,
)
from snuba.query.processors.logical.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.utils.schemas import AggregateFunction
from snuba.utils.schemas import Column as SchemaColumn
from snuba.utils.schemas import DateTime, Nested, UInt
from snuba.web import QueryResult


@pytest.fixture
def start_time() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=pytz.utc
    )


@pytest.fixture
def end_time(start_time: datetime) -> datetime:
    return start_time + timedelta(minutes=10)


@pytest.fixture
def pluggable_sets_entity() -> PluggableEntity:
    return PluggableEntity(
        name="generic_metrics_sets",
        readable_storage=get_storage(StorageKey.GENERIC_METRICS_SETS),
        query_processors=[
            TagsTypeTransformer(),
            MappedGranularityProcessor(
                accepted_granularities=PERFORMANCE_GRANULARITIES,
                default_granularity=DEFAULT_MAPPED_GRANULARITY_ENUM,
            ),
            TimeSeriesProcessor({"bucketed_time": "timestamp"}, ("timestamp",)),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ],
        columns=[
            SchemaColumn("org_id", UInt(64)),
            SchemaColumn("project_id", UInt(64)),
            SchemaColumn("metric_id", UInt(64)),
            SchemaColumn("timestamp", DateTime()),
            SchemaColumn("bucketed_time", DateTime()),
            SchemaColumn("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
            SchemaColumn("value", AggregateFunction("uniqCombined64", [UInt(64)])),
        ],
        translation_mappers=TranslationMappers(
            subscriptables=[
                SubscriptableMapper(
                    from_column_table=None,
                    from_column_name="tags_raw",
                    to_nested_col_table=None,
                    to_nested_col_name="tags",
                    value_subcolumn_name="raw_value",
                ),
                SubscriptableMapper(
                    from_column_table=None,
                    from_column_name="tags",
                    to_nested_col_table=None,
                    to_nested_col_name="tags",
                    value_subcolumn_name="indexed_value",
                ),
            ],
            functions=[
                FunctionNameMapper("uniq", "uniqCombined64Merge"),
                FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
            ],
        ),
        validators=[],
        required_time_column="timestamp",
    )


def build_request(query_body: Mapping[str, str]) -> Request:
    generic_metrics_dataset = get_dataset("generic_metrics")
    query, snql_anonymized = parse_snql_query(
        query_body["query"], generic_metrics_dataset
    )
    request = Request(
        id="",
        original_body=query_body,
        query=query,
        snql_anonymized=snql_anonymized,
        query_settings=HTTPQuerySettings(referrer=""),
        attribution_info=AttributionInfo(get_app_id("blah"), "blah", None, None, None),
    )
    return request


def test_generic_metrics_sets_vs_pluggable_similar_pipeline_behavior(
    start_time: datetime, end_time: datetime, pluggable_sets_entity: PluggableEntity
) -> None:
    query_body = {
        "query": f"""
            MATCH (generic_metrics_sets)
            SELECT uniq(value) AS unique_values BY project_id, org_id
            WHERE org_id = 1
            AND project_id = 2
            AND metric_id = 3
            AND tags['a'] = 4
            AND timestamp >= toDateTime('{start_time}')
            AND timestamp < toDateTime('{end_time}')
            GRANULARITY 60
        """,
        "dataset": "generic_metrics",
    }

    # the pipeline modifies the query in the request so we need to construct
    # two separate ones
    request = build_request(query_body=query_body)
    request2 = build_request(query_body=query_body)

    sets_entity: GenericMetricsSetsEntity = GenericMetricsSetsEntity()

    RESULT_MAP: MutableMapping[str, Query] = {}

    def query_runner(
        query: Query, settings: QuerySettings, reader: Reader, output_name: str
    ) -> QueryResult:
        RESULT_MAP[output_name] = query
        return QueryResult({}, {"experiments": {}, "sql": "", "stats": {}})

    sets_entity.get_query_pipeline_builder().build_execution_pipeline(
        request=request, runner=partial(query_runner, output_name="existing")
    ).execute()

    pluggable_sets_entity.get_query_pipeline_builder().build_execution_pipeline(
        request=request2, runner=partial(query_runner, output_name="pluggable")
    ).execute()

    (match, failure_test) = RESULT_MAP["existing"].equals(RESULT_MAP["pluggable"])
    assert match, failure_test
