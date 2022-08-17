from __future__ import annotations

import sys
from typing import Any, Mapping, Sequence, Type

from snuba.clickhouse.translators.snuba.allowed import (
    FunctionCallMapper,
    SubscriptableReferenceMapper,
)
from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
    SubscriptableMapper,
)
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
from snuba.query.validation.validators import (
    EntityRequiredColumnValidator,
    QueryValidator,
)

QP_MAPPING: Mapping[str, Type[QueryProcessor]] = {
    "transform_tag_types": TagsTypeTransformer,
    "handle_mapped_granularities": MappedGranularityProcessor,
    "translate_time_series": TimeSeriesProcessor,
    "referrer_rate_limit": ReferrerRateLimiterProcessor,
    "org_rate_limiter": OrganizationRateLimiterProcessor,
    "project_referrer_rate_limiter": ProjectReferrerRateLimiter,
    "project_rate_limiter": ProjectRateLimiterProcessor,
    "resource_quota_limiter": ResourceQuotaProcessor,
}

FUNCTION_MAPPER_MAPPING: Mapping[str, Type[FunctionCallMapper]] = {
    "simple_func": FunctionNameMapper
}

SUB_MAPPER_MAPPING: Mapping[str, Type[SubscriptableReferenceMapper]] = {
    "subscriptable": SubscriptableMapper
}

VALIDATOR_MAPPING: Mapping[str, Type[QueryValidator]] = {
    "entity_required": EntityRequiredColumnValidator
}


def _build_entity_validators(
    config_validators: list[dict[str, Any]]
) -> Sequence[QueryValidator]:
    return [
        VALIDATOR_MAPPING[qv_config["validator"]](**qv_config["args"])  # type: ignore
        for qv_config in config_validators
    ]


def _build_entity_query_processors(
    config_query_processors: list[dict[str, Any]],
) -> Sequence[QueryProcessor]:
    return [QP_MAPPING[config_qp["processor"]](**(config_qp["args"] if config_qp.get("args") else {})) for config_qp in config_query_processors]  # type: ignore


def _build_entity_translation_mappers(
    config_translation_mappers: dict[str, Any],
) -> TranslationMappers:
    function_mappers: list[FunctionCallMapper] = [
        FUNCTION_MAPPER_MAPPING[fm_config["mapper"]](**fm_config["args"])  # type: ignore
        for fm_config in config_translation_mappers["functions"]
    ]
    subscriptable_mappers: list[SubscriptableReferenceMapper] = [
        SUB_MAPPER_MAPPING[sub_config["mapper"]](**sub_config["args"])  # type: ignore
        for sub_config in config_translation_mappers["subscriptables"]
    ]
    return TranslationMappers(
        functions=function_mappers, subscriptables=subscriptable_mappers
    )


def build_entity_from_config(file_path: str) -> Entity:
    config_data = load_configuration_data(file_path, {"entity": V1_ENTITY_SCHEMA})
    return PluggableEntity(
        name=config_data["name"],
        query_processors=_build_entity_query_processors(
            config_data["query_processors"]
        ),
        columns=parse_columns(config_data["schema"]),
        readable_storage=get_storage(StorageKey(config_data["readable_storage"])),
        validators=_build_entity_validators(config_data["validators"]),
        translation_mappers=_build_entity_translation_mappers(
            config_data["translation_mappers"]
        ),
        writeable_storage=get_writable_storage(
            StorageKey(config_data["writable_storage"])
        )
        if "writable_storage" in config_data
        else None,
    )


if __name__ == "__main__":
    entity = build_entity_from_config(sys.argv[1])
    # breakpoint()
