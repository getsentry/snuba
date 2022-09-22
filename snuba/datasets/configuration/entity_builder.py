from __future__ import annotations

import logging
from typing import Any, Sequence, Type

import snuba.clickhouse.translators.snuba.function_call_mappers  # noqa
from snuba.clickhouse.translators.snuba.allowed import (
    CurriedFunctionCallMapper,
    FunctionCallMapper,
    SubscriptableReferenceMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.configuration.json_schema import V1_ENTITY_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.configuration.utils import parse_columns
from snuba.datasets.entities.entity_key import register_entity_key
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.granularity_processor import (
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
from snuba.query.validation.validators import QueryValidator

# TODO replace all the explicit mapping dictionaries below with the
# registered class factory pattern (e.g. https://github.com/getsentry/snuba/pull/3044)
_QP_MAPPING: dict[str, Type[LogicalQueryProcessor]] = {
    "transform_tag_types": TagsTypeTransformer,
    "handle_mapped_granularities": MappedGranularityProcessor,
    "translate_time_series": TimeSeriesProcessor,
    "referrer_rate_limit": ReferrerRateLimiterProcessor,
    "org_rate_limiter": OrganizationRateLimiterProcessor,
    "project_referrer_rate_limiter": ProjectReferrerRateLimiter,
    "project_rate_limiter": ProjectRateLimiterProcessor,
    "resource_quota_limiter": ResourceQuotaProcessor,
}

logger = logging.getLogger("snuba.entity_builder")


def _build_entity_validators(
    config_validators: list[dict[str, Any]]
) -> Sequence[QueryValidator]:
    return [
        QueryValidator.get_from_name(qv_config["validator"])(**qv_config["args"])
        for qv_config in config_validators
    ]


def _build_entity_query_processors(
    config_query_processors: list[dict[str, Any]],
) -> Sequence[LogicalQueryProcessor]:
    return [
        _QP_MAPPING[config_qp["processor"]](
            **(config_qp["args"] if config_qp.get("args") else {})
        )
        for config_qp in config_query_processors
    ]


def _build_entity_translation_mappers(
    config_translation_mappers: dict[str, Any],
) -> TranslationMappers:
    function_mappers: list[FunctionCallMapper] = [
        FunctionCallMapper.get_from_name(fm_config["mapper"])(**fm_config["args"])
        for fm_config in config_translation_mappers["functions"]
    ]
    subscriptable_mappers: list[SubscriptableReferenceMapper] = [
        SubscriptableReferenceMapper.get_from_name(sub_config["mapper"])(
            **sub_config["args"]
        )
        for sub_config in config_translation_mappers["subscriptables"]
    ]
    curried_function_mappers: list[CurriedFunctionCallMapper] = (
        [
            CurriedFunctionCallMapper.get_from_name(fm_config["mapper"])(
                **fm_config["args"]
            )
            for fm_config in config_translation_mappers["curried_functions"]
        ]
        if "curried_functions" in config_translation_mappers
        else []
    )
    return TranslationMappers(
        functions=function_mappers,
        subscriptables=subscriptable_mappers,
        curried_functions=curried_function_mappers,
    )


def build_entity_from_config(file_path: str) -> PluggableEntity:
    config_data = load_configuration_data(file_path, {"entity": V1_ENTITY_SCHEMA})
    return PluggableEntity(
        entity_key=register_entity_key(config_data["name"]),
        query_processors=_build_entity_query_processors(
            config_data["query_processors"]
        ),
        columns=parse_columns(config_data["schema"]),
        readable_storage=get_storage(StorageKey(config_data["readable_storage"])),
        required_time_column=config_data["required_time_column"],
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
