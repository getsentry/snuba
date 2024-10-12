import logging
from typing import Any, Dict, Mapping, Optional, Sequence

import rapidjson
from confluent_kafka import KafkaError
from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer

from snuba import environment, settings
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storage import WritableTableStorage
from snuba.query.conditions import combine_or_conditions
from snuba.query.data_source.simple import Table
from snuba.query.dsl import literal
from snuba.query.exceptions import InvalidQueryException, NoRowsToDeleteException
from snuba.query.expressions import Expression
from snuba.reader import Result
from snuba.state import get_int_config
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.schemas import ColumnValidator, InvalidColumnType
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic
from snuba.web.delete_query import (
    DeletesNotEnabledError,
    TooManyOngoingMutationsError,
    _construct_condition,
    _enforce_max_rows,
    _get_attribution_info,
    _num_ongoing_mutations,
    deletes_are_enabled,
)

metrics = MetricsWrapper(environment.metrics, "snuba.delete")
logger = logging.getLogger(__name__)

delete_kfk: Producer | None = None


def _delete_kafka_producer() -> Producer:
    global delete_kfk
    if delete_kfk is None:
        delete_kfk = Producer(
            build_kafka_producer_configuration(
                topic=Topic.LW_DELETIONS,
            )
        )
    return delete_kfk


def _record_query_delivery_callback(
    error: Optional[KafkaError], message: KafkaMessage
) -> None:
    metrics.increment(
        "record_query.delivery_callback",
        tags={"status": "success" if error is None else "failure"},
    )

    if error is not None:
        logger.warning("Could not produce delete query due to error: %r", error)


def produce_delete_query(delete_query: Mapping) -> None:
    try:
        producer = _delete_kafka_producer()
        data = rapidjson.dumps(delete_query)
        producer.poll(0)  # trigger queued delivery callbacks
        producer.produce(
            settings.KAFKA_TOPIC_MAP.get(
                Topic.LW_DELETIONS.value, Topic.LW_DELETIONS.value
            ),
            data.encode("utf-8"),
            on_delivery=_record_query_delivery_callback,
            # TODO(should we add headers? or key)
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
    Copied over from delete_query.py with some modifications
    """
    if not deletes_are_enabled():
        raise DeletesNotEnabledError("Deletes not enabled in this region")

    delete_settings = storage.get_deletion_settings()
    if not delete_settings.is_enabled:
        raise DeletesNotEnabledError(
            f"Deletes not enabled for {storage.get_storage_key().value}"
        )

    if list(conditions.keys()).sort() != delete_settings.allowed_columns.sort():
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

    # fail if too many mutations ongoing
    ongoing_mutations = _num_ongoing_mutations(
        storage.get_cluster(), delete_settings.tables
    )
    max_ongoing_mutations = get_int_config(
        "MAX_ONGOING_MUTATIONS_FOR_DELETE",
        default=settings.MAX_ONGOING_MUTATIONS_FOR_DELETE,
    )
    assert max_ongoing_mutations
    if ongoing_mutations > max_ongoing_mutations:
        raise TooManyOngoingMutationsError(
            f"max ongoing mutations to do a delete is {max_ongoing_mutations}, but at least one replica has {ongoing_mutations} ongoing"
        )

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
) -> Result:

    highest_rows_to_delete = 1  # FOR TESTING ONLY
    result: Result = {}
    for table in tables:
        query = construct_query(storage, table, _construct_condition(conditions))
        try:
            num_rows_to_delete = _enforce_max_rows(query)
            highest_rows_to_delete = max(highest_rows_to_delete, num_rows_to_delete)
            result[table] = {"rows_to_delete": num_rows_to_delete}
        except NoRowsToDeleteException:
            result[table] = {}

    if highest_rows_to_delete == 0:
        return result

    delete_query = {
        "num_rows": highest_rows_to_delete,
        "storage_name": storage.get_storage_key().value,
        "conditions": conditions,
        # TODO do we need attribution info?
    }
    produce_delete_query(delete_query)
    return result


def construct_or_conditions(conditions: Sequence[Dict[str, Any]]) -> Expression:
    """
    Combines multiple AND conditions: (equals(project_id, 1) AND in(group_id, (2, 3, 4, 5))
    into OR conditions for a bulk delete
    """
    or_conditions = []
    for cond in conditions:
        and_condition = _construct_condition(cond)
        or_conditions.append(and_condition)
    return combine_or_conditions(or_conditions)
