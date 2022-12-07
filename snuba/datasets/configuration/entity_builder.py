from __future__ import annotations

from typing import Any, Sequence

import snuba.clickhouse.translators.snuba.function_call_mappers  # noqa
from snuba.clickhouse.translators.snuba.allowed import (
    ColumnMapper,
    CurriedFunctionCallMapper,
    FunctionCallMapper,
    SubscriptableReferenceMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.configuration.json_schema import ENTITY_VALIDATORS
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.configuration.utils import parse_columns
from snuba.datasets.entities.entity_key import register_entity_key
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.validation.validators import QueryValidator


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
        LogicalQueryProcessor.get_from_name(config_qp["processor"]).from_kwargs(
            **(config_qp["args"] if config_qp.get("args") else {})
        )
        for config_qp in config_query_processors
    ]


def _build_entity_translation_mappers(
    config_translation_mappers: dict[str, Any],
) -> TranslationMappers:
    columns_mappers: list[ColumnMapper] = (
        [
            ColumnMapper.get_from_name(col_config["mapper"])(**col_config["args"])
            for col_config in config_translation_mappers["columns"]
        ]
        if "columns" in config_translation_mappers
        else []
    )
    function_mappers: list[FunctionCallMapper] = (
        [
            FunctionCallMapper.get_from_name(fm_config["mapper"])(**fm_config["args"])
            for fm_config in config_translation_mappers["functions"]
        ]
        if "functions" in config_translation_mappers
        else []
    )
    subscriptable_mappers: list[SubscriptableReferenceMapper] = (
        [
            SubscriptableReferenceMapper.get_from_name(sub_config["mapper"])(
                **sub_config["args"]
            )
            for sub_config in config_translation_mappers["subscriptables"]
        ]
        if "subscriptables" in config_translation_mappers
        else []
    )
    curried_function_mappers: list[CurriedFunctionCallMapper] = (
        [
            CurriedFunctionCallMapper.get_from_name(curr_config["mapper"])(
                **curr_config["args"]
            )
            for curr_config in config_translation_mappers["curried_functions"]
        ]
        if "curried_functions" in config_translation_mappers
        else []
    )
    return TranslationMappers(
        columns=columns_mappers,
        functions=function_mappers,
        subscriptables=subscriptable_mappers,
        curried_functions=curried_function_mappers,
    )


def build_entity_from_config(file_path: str) -> PluggableEntity:
    config = load_configuration_data(file_path, ENTITY_VALIDATORS)
    return PluggableEntity(
        entity_key=register_entity_key(config["name"]),
        query_processors=_build_entity_query_processors(config["query_processors"]),
        columns=parse_columns(config["schema"]),
        readable_storage=get_storage(StorageKey(config["readable_storage"])),
        required_time_column=config["required_time_column"],
        validators=_build_entity_validators(config["validators"]),
        translation_mappers=_build_entity_translation_mappers(
            config["translation_mappers"]
        ),
        writeable_storage=get_writable_storage(StorageKey(config["writable_storage"]))
        if "writable_storage" in config
        else None,
        partition_key_column_name=config.get("partition_key_column_name", None),
    )
