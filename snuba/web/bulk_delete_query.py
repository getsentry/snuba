import logging
import time
from threading import Thread
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence, TypedDict

import rapidjson
from confluent_kafka import KafkaError, Producer
from confluent_kafka import Message as KafkaMessage

from snuba import environment, settings
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.deletion_settings import DeletionSettings, get_trace_item_type_name
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.types import AttributeConditions, ConditionsBag, ConditionsType
from snuba.query.conditions import combine_or_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import literal
from snuba.query.exceptions import InvalidQueryException, NoRowsToDeleteException
from snuba.query.expressions import Expression
from snuba.reader import Result
from snuba.state import get_int_config, get_str_config
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.schemas import ColumnValidator, InvalidColumnType
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic
from snuba.web.delete_query import (
    DeletesNotEnabledError,
    _construct_condition,
    _enforce_max_rows,
    _get_attribution_info,
    deletes_are_enabled,
)

metrics = MetricsWrapper(environment.metrics, "snuba.delete")
logger = logging.getLogger(__name__)


class WireAttributeCondition(TypedDict):
    attr_key_type: int
    attr_key_name: str
    attr_values: Sequence[bool | str | int | float]


class DeleteQueryMessage(TypedDict, total=False):
    rows_to_delete: int
    storage_name: str
    conditions: ConditionsType
    tenant_ids: Mapping[str, str | int]
    attribute_conditions: Optional[Dict[str, WireAttributeCondition]]
    attribute_conditions_item_type: Optional[int]


PRODUCER_MAP: MutableMapping[str, Producer] = {}
STORAGE_TOPIC: Mapping[str, Topic] = {
    StorageKey.SEARCH_ISSUES.value: Topic.LW_DELETIONS_GENERIC_EVENTS,
    StorageKey.EAP_ITEMS.value: Topic.LW_DELETIONS_EAP_ITEMS,
}


class InvalidStorageTopic(Exception):
    pass


def _get_kafka_producer(topic: Topic) -> Producer:
    producer = PRODUCER_MAP.get(topic.value)
    if not producer:
        producer = Producer(
            build_kafka_producer_configuration(
                topic=topic,
            )
        )
        PRODUCER_MAP[topic.value] = producer
    return producer


def flush_producers() -> None:
    """
    It's not guaranteed that there will be a steady stream of
    DELETE requests so we can call producer.flush() for any active
    producers to make sure the delivery callbacks are called.
    """

    def _flush_producers() -> None:
        while True:
            for storage, producer in PRODUCER_MAP.items():
                messages_remaining = producer.flush(5.0)
                if messages_remaining:
                    logger.debug(f"{messages_remaining} {storage} messages pending delivery")
            time.sleep(1)

    Thread(target=_flush_producers, name="flush_producers", daemon=True).start()


def _delete_query_delivery_callback(error: Optional[KafkaError], message: KafkaMessage) -> None:
    metrics.increment(
        "delete_query.delivery_callback",
        tags={"status": "failure" if error else "success"},
    )

    if error is not None:
        logger.warning("Could not produce delete query due to error: %r", error)


FLUSH_PRODUCERS_THREAD_STARTED = False


def produce_delete_query(delete_query: DeleteQueryMessage) -> None:
    global FLUSH_PRODUCERS_THREAD_STARTED
    if not FLUSH_PRODUCERS_THREAD_STARTED:
        FLUSH_PRODUCERS_THREAD_STARTED = True
        flush_producers()

    storage_name = delete_query["storage_name"]
    topic = STORAGE_TOPIC.get(storage_name)
    if not topic:
        raise InvalidStorageTopic(f"No topic found for {storage_name}")
    try:
        producer = _get_kafka_producer(topic)
        data = rapidjson.dumps(delete_query).encode("utf-8")
        producer.poll(0)  # trigger queued delivery callbacks
        producer.produce(
            settings.KAFKA_TOPIC_MAP.get(topic.value, topic.value),
            data,
            on_delivery=_delete_query_delivery_callback,
        )
    except Exception as ex:
        logger.exception("Could not produce delete query due to error: %r", ex)


def _validate_attribute_conditions(
    attribute_conditions: AttributeConditions,
    delete_settings: DeletionSettings,
) -> None:
    """
    Validates that the attribute_conditions are allowed for the configured item_type.

    Args:
        attribute_conditions: AttributeConditions containing item_type and attribute mappings
        delete_settings: The deletion settings for the storage

    Raises:
        InvalidQueryException: If no attributes are configured for the item_type,
                              or if any requested attributes are not allowed
    """
    allowed_attrs_config = delete_settings.allowed_attributes_by_item_type

    if not allowed_attrs_config:
        raise InvalidQueryException("No attribute-based deletions configured for this storage")

    # Map the integer item_type to its string name used in configuration
    try:
        item_type_name = get_trace_item_type_name(attribute_conditions.item_type)
    except ValueError as e:
        raise InvalidQueryException(str(e))

    # Check if this specific item_type has any allowed attributes configured
    if item_type_name not in allowed_attrs_config:
        raise InvalidQueryException(
            f"No attribute-based deletions configured for item_type {item_type_name} "
            f"(value: {attribute_conditions.item_type}). Configured item types: "
            f"{sorted(allowed_attrs_config.keys())}"
        )

    # Get the allowed attributes for this specific item_type
    allowed_attrs = allowed_attrs_config[item_type_name]

    # Validate that all requested attributes are allowed
    requested_attrs = set(attribute_conditions.attributes.keys())
    allowed_attrs_set = set(allowed_attrs)
    invalid_attrs = requested_attrs - allowed_attrs_set

    if invalid_attrs:
        raise InvalidQueryException(
            f"Invalid attributes for deletion on item_type '{item_type_name}': {invalid_attrs}. "
            f"Allowed attributes: {allowed_attrs_set}"
        )


@with_span()
def delete_from_storage(
    storage: WritableTableStorage,
    column_conditions: Dict[str, list[Any]],
    attribution_info: Mapping[str, Any],
    attribute_conditions: Optional[AttributeConditions] = None,
) -> dict[str, Result]:
    """
    This method does a series of validation checks (outline below),
    before `delete_from_tables` produces the delete query messages
    to the appropriate topic.

    * runtime flag validation `storage_deletes_enabled` (done by region)
    * storage validation that deletes are enabled
    * column names are valid (allowed_columns storage setting)
    * column types are valid
    * attribute names are valid (allowed_attributes_by_item_type storage setting)
    """
    if not deletes_are_enabled():
        raise DeletesNotEnabledError("Deletes not enabled in this region")

    delete_settings = storage.get_deletion_settings()
    if not delete_settings.is_enabled:
        raise DeletesNotEnabledError(f"Deletes not enabled for {storage.get_storage_key().value}")

    columns_diff = set(column_conditions.keys()) - set(delete_settings.allowed_columns)
    if columns_diff != set():
        raise InvalidQueryException(
            f"Invalid Columns to filter by, must be in {delete_settings.allowed_columns}"
        )

    # validate column types
    columns = storage.get_schema().get_columns()
    column_validator = ColumnValidator(columns)
    try:
        for col, values in column_conditions.items():
            column_validator.validate(col, values)
    except InvalidColumnType as e:
        raise InvalidQueryException(e.message)

    # validate attribute conditions if provided
    if attribute_conditions:
        _validate_attribute_conditions(attribute_conditions, delete_settings)

        if not get_int_config("permit_delete_by_attribute", default=0):
            metrics.increment("delete_query.delete_ignored")
            return {}

    attr_info = _get_attribution_info(attribution_info)
    return delete_from_tables(
        storage,
        delete_settings.tables,
        ConditionsBag(
            column_conditions=column_conditions,
            attribute_conditions=attribute_conditions,
        ),
        attr_info,
    )


def construct_query(storage: WritableTableStorage, table: str, condition: Expression) -> Query:
    cluster_name = storage.get_cluster().get_clickhouse_cluster_name()
    on_cluster = literal(cluster_name) if cluster_name else None
    return Query(
        from_clause=Table(
            table,
            ColumnSet([]),
            storage_key=storage.get_storage_key(),
            allocation_policies=storage.get_delete_allocation_policies(),
        ),
        condition=condition,
        on_cluster=on_cluster,
        is_delete=True,
    )


def _serialize_attribute_conditions(
    attribute_conditions: AttributeConditions,
) -> Dict[str, WireAttributeCondition]:
    result: Dict[str, WireAttributeCondition] = {}
    for key, (attr_key_enum, values) in attribute_conditions.attributes.items():
        result[key] = {
            "attr_key_type": attr_key_enum.type,
            "attr_key_name": attr_key_enum.name,
            "attr_values": values,
        }
    return result


def delete_from_tables(
    storage: WritableTableStorage,
    tables: Sequence[str],
    conditions: ConditionsBag,
    attribution_info: AttributionInfo,
) -> dict[str, Result]:
    highest_rows_to_delete = 0
    result: dict[str, Result] = {}
    for table in tables:
        where_clause = _construct_condition(storage, conditions)
        query = construct_query(storage, table, where_clause)
        try:
            num_rows_to_delete = _enforce_max_rows(query)
            highest_rows_to_delete = max(highest_rows_to_delete, num_rows_to_delete)
            result[table] = {"data": [{"rows_to_delete": num_rows_to_delete}]}
        except NoRowsToDeleteException:
            result[table] = {}

    if highest_rows_to_delete == 0:
        return result

    storage_name = storage.get_storage_key().value
    project_id = attribution_info.tenant_ids.get("project_id")
    if project_id and should_use_killswitch(storage_name, str(project_id)):
        return result

    delete_query: DeleteQueryMessage = {
        "rows_to_delete": highest_rows_to_delete,
        "storage_name": storage_name,
        "conditions": conditions.column_conditions,
        "tenant_ids": attribution_info.tenant_ids,
    }

    # Add attribute_conditions to the message if present
    if conditions.attribute_conditions:
        delete_query["attribute_conditions"] = _serialize_attribute_conditions(
            conditions.attribute_conditions
        )
        delete_query["attribute_conditions_item_type"] = conditions.attribute_conditions.item_type

    produce_delete_query(delete_query)
    return result


def construct_or_conditions(
    storage: WritableTableStorage,
    conditions: Sequence[ConditionsBag],
) -> Expression:
    """
    Combines multiple AND conditions: (equals(project_id, 1) AND in(group_id, (2, 3, 4, 5))
    into OR conditions for a bulk delete
    """
    return combine_or_conditions([_construct_condition(storage, cond) for cond in conditions])


def should_use_killswitch(storage_name: str, project_id: str) -> bool:
    killswitch_config = get_str_config(f"lw_deletes_killswitch_{storage_name}", default="")
    return project_id in killswitch_config if killswitch_config else False
