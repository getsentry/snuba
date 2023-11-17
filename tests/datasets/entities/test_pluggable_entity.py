from datetime import datetime, timedelta, timezone
from typing import Mapping

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.factory import get_dataset
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
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
from snuba.query.processors.logical.tags_type_transformer import TagsTypeTransformer
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Request
from snuba.utils.schemas import AggregateFunction
from snuba.utils.schemas import Column as SchemaColumn
from snuba.utils.schemas import DateTime, Nested, UInt


@pytest.fixture
def start_time() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


@pytest.fixture
def end_time(start_time: datetime) -> datetime:
    return start_time + timedelta(minutes=10)


@pytest.fixture
def pluggable_sets_entity() -> PluggableEntity:
    return PluggableEntity(
        entity_key=EntityKey.GENERIC_METRICS_SETS,
        storages=[
            EntityStorageConnection(
                get_storage(StorageKey.GENERIC_METRICS_SETS),
                TranslationMappers(
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
            )
        ],
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
        validators=[],
        required_time_column="timestamp",
        storage_selector=DefaultQueryStorageSelector(),
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
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    return request
