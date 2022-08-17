from __future__ import annotations

import sys
from typing import Any, Mapping, Sequence, Type

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.configuration.json_schema import V1_ENTITY_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.configuration.utils import parse_columns
from snuba.datasets.entities.metrics import TagsTypeTransformer
from snuba.datasets.entity import Entity
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.processors import QueryProcessor
from snuba.query.processors.granularity_processor import MappedGranularityProcessor
from snuba.query.processors.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor

QP_MAPPING: Mapping[str, Type[QueryProcessor]] = {
    "transform_tag_types": TagsTypeTransformer,
    "handle_mapped_granularities": MappedGranularityProcessor,
    "translate_time_series": TimeSeriesProcessor,
    "referrer_rate_limit": ReferrerRateLimiterProcessor,
    "org_rate_limiter_processor": OrganizationRateLimiterProcessor,
    "project_referrer_rate_limiter": ProjectReferrerRateLimiter,
    "project_rate_limiter": ProjectRateLimiterProcessor,
    "resource_quota_limiter": ResourceQuotaProcessor,
}


def get_entity_query_processors(
    config_query_processors: list[dict[str, Any]],
) -> Sequence[QueryProcessor]:
    return [QP_MAPPING[config_qp["processor"]](**(config_qp["args"] if config_qp.get("args") else {})) for config_qp in config_query_processors]  # type: ignore


def build_entity_from_config(file_path: str) -> Entity:
    config_data = load_configuration_data(file_path, {"entity": V1_ENTITY_SCHEMA})
    print(config_data)
    return PluggableEntity(
        query_processors=get_entity_query_processors(config_data["query_processors"]),
        columns=parse_columns(config_data["schema"]),
        readable_storage=get_storage(StorageKey(config_data["readable_storage"])),
        validators=[],
        translation_mappers=TranslationMappers(),
        writeable_storage=get_writable_storage(
            StorageKey(config_data["writable_storage"])
        )
        if "writable_storage" in config_data
        else None,
    )


if __name__ == "__main__":
    entity = build_entity_from_config(sys.argv[1])
    # breakpoint()
