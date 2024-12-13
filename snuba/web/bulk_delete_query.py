import logging
import time
from threading import Thread
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence, TypedDict

import rapidjson
from confluent_kafka import KafkaError
from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer

from snuba import environment, settings
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import combine_or_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import literal
from snuba.query.exceptions import InvalidQueryException, NoRowsToDeleteException
from snuba.query.expressions import Expression
from snuba.reader import Result
from snuba.state import get_str_config
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.schemas import ColumnValidator, InvalidColumnType
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic
from snuba.web.delete_query import (
    ConditionsType,
    DeletesNotEnabledError,
    _construct_condition,
    _enforce_max_rows,
    _get_attribution_info,
    deletes_are_enabled,
)

metrics = MetricsWrapper(environment.metrics, "snuba.delete")
logger = logging.getLogger(__name__)


class DeleteQueryMessage(TypedDict):
    rows_to_delete: int
    storage_name: str
    conditions: ConditionsType
    tenant_ids: Mapping[str, str | int]


PRODUCER_MAP: MutableMapping[str, Producer] = {}
STORAGE_TOPIC: Mapping[str, Topic] = {
    StorageKey.SEARCH_ISSUES.value: Topic.LW_DELETIONS_GENERIC_EVENTS
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
                    logger.debug(
                        f"{messages_remaining} {storage} messages pending delivery"
                    )
            time.sleep(1)

    Thread(target=_flush_producers, name="flush_producers", daemon=True).start()


def _delete_query_delivery_callback(
    error: Optional[KafkaError], message: KafkaMessage
) -> None:
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


@with_span()
def delete_from_storage(
    storage: WritableTableStorage,
    conditions: Dict[str, list[Any]],
    attribution_info: Mapping[str, Any],
) -> dict[str, Result]:
    """
    This method does a series of validation checks (outline below),
    before `delete_from_tables` produces the delete query messages
    to the appropriate topic.

    * runtime flag validation `storage_deletes_enabled` (done by region)
    * storage validation that deletes are enabled
    * column names are valid (allowed_columns storage setting)
    * column types are valid
    """
    if not deletes_are_enabled():
        raise DeletesNotEnabledError("Deletes not enabled in this region")

    delete_settings = storage.get_deletion_settings()
    if not delete_settings.is_enabled:
        raise DeletesNotEnabledError(
            f"Deletes not enabled for {storage.get_storage_key().value}"
        )

    columns_diff = set(conditions.keys()) - set(delete_settings.allowed_columns)
    if columns_diff != set():
        raise InvalidQueryException(
            f"Invalid Columns to filter by, must be in {delete_settings.allowed_columns}"
        )

    # validate column types
    columns = storage.get_schema().get_columns()
    column_validator = ColumnValidator(columns)
    try:
        for col, values in conditions.items():
            column_validator.validate(col, values)
    except InvalidColumnType as e:
        raise InvalidQueryException(e.message)

    attr_info = _get_attribution_info(attribution_info)
    return delete_from_tables(storage, delete_settings.tables, conditions, attr_info)


def construct_query(
    storage: WritableTableStorage, table: str, condition: Expression
) -> Query:
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


def delete_from_tables(
    storage: WritableTableStorage,
    tables: Sequence[str],
    conditions: Dict[str, Any],
    attribution_info: AttributionInfo,
) -> dict[str, Result]:

    highest_rows_to_delete = 0
    result: dict[str, Result] = {}
    for table in tables:
        query = construct_query(storage, table, _construct_condition(conditions))
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
        "conditions": conditions,
        "tenant_ids": attribution_info.tenant_ids,
    }
    produce_delete_query(delete_query)
    return result


def construct_or_conditions(conditions: Sequence[ConditionsType]) -> Expression:
    """
    Combines multiple AND conditions: (equals(project_id, 1) AND in(group_id, (2, 3, 4, 5))
    into OR conditions for a bulk delete
    """
    return combine_or_conditions([_construct_condition(cond) for cond in conditions])


def should_use_killswitch(storage_name: str, project_id: str) -> bool:
    killswitch_config = get_str_config(
        f"lw_deletes_killswitch_{storage_name}", default=""
    )
    return project_id in killswitch_config if killswitch_config else False
