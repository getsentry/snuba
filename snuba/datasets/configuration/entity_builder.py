from __future__ import annotations

from typing import Any, Optional, Sequence

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
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelector
from snuba.datasets.entity_subscriptions.processors import EntitySubscriptionProcessor
from snuba.datasets.entity_subscriptions.validators import EntitySubscriptionValidator
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.join import ColumnEquivalence, JoinRelationship, JoinType
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.validation.validators import QueryValidator


class InvalidEntityConfigException(Exception):
    pass


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


def _build_storage_selector(
    config_storage_selector: dict[str, Any]
) -> QueryStorageSelector:
    return QueryStorageSelector.get_from_name(config_storage_selector["selector"])()


def _build_subscription_processors(
    config: dict[str, Any]
) -> Optional[Sequence[EntitySubscriptionProcessor]]:
    if "subscription_processors" in config:
        processors: Sequence[EntitySubscriptionProcessor] = [
            EntitySubscriptionProcessor.get_from_name(pro_config["processor"])(
                **pro_config["args"]
            )
            for pro_config in config["subscription_processors"]
        ]
        return processors
    return None


def _build_subscription_validators(
    config: dict[str, Any]
) -> Optional[Sequence[EntitySubscriptionValidator]]:
    if "subscription_validators" in config:
        validators: Sequence[EntitySubscriptionValidator] = [
            EntitySubscriptionValidator.get_from_name(val_config["validator"])(
                **val_config["args"]
            )
            for val_config in config["subscription_validators"]
        ]
        return validators
    return None


def _build_join_relationships(config: dict[str, Any]) -> dict[str, JoinRelationship]:
    relationships: dict[str, JoinRelationship] = {}
    if "join_relationships" not in config:
        return relationships

    for key, obj in config["join_relationships"].items():
        rhs_key = register_entity_key(obj["rhs_entity"])
        columns = [(c[0], c[1]) for c in obj["columns"]]
        equivalences = []
        for pair in obj.get("equivalences", []):
            equivalences.append(ColumnEquivalence(pair[0], pair[1]))

        if obj["join_type"] not in ("inner", "left"):
            raise InvalidEntityConfigException(
                f"{obj['join_type']} is not a valid join type"
            )

        join_type = JoinType.LEFT if obj["join_type"] == "left" else JoinType.INNER
        join = JoinRelationship(
            rhs_entity=rhs_key,
            columns=columns,
            join_type=join_type,
            equivalences=equivalences,
        )
        relationships[key] = join

    return relationships


def build_entity_from_config(file_path: str) -> PluggableEntity:
    config = load_configuration_data(file_path, ENTITY_VALIDATORS)
    return PluggableEntity(
        entity_key=register_entity_key(config["name"]),
        storage_selector=_build_storage_selector(config["storage_selector"]),
        query_processors=_build_entity_query_processors(config["query_processors"]),
        columns=parse_columns(config["schema"]),
        readable_storage=get_storage(StorageKey(config["readable_storage"])),
        required_time_column=config["required_time_column"],
        validators=_build_entity_validators(config["validators"]),
        translation_mappers=_build_entity_translation_mappers(
            config.get("translation_mappers", {})
        ),
        join_relationships=_build_join_relationships(config),
        writeable_storage=get_writable_storage(StorageKey(config["writable_storage"]))
        if "writable_storage" in config
        else None,
        partition_key_column_name=config.get("partition_key_column_name", None),
        subscription_processors=_build_subscription_processors(config),
        subscription_validators=_build_subscription_validators(config),
    )
