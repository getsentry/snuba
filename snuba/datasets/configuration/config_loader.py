from __future__ import annotations

from typing import Any, Callable

from arroyo import Topic as KafkaTopic
from arroyo.backends.kafka import KafkaProducer
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueuePolicy,
    ProduceInvalidMessagePolicy,
)

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    Float,
    Nested,
    SchemaModifiers,
    String,
    UInt,
)
from snuba.datasets.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
)
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.storage import ReadableStorage
from snuba.utils.schemas import UUID, AggregateFunction
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


def policy_creator_creator(
    dlq_policy_conf: dict[str, Any],
) -> Callable[[], DeadLetterQueuePolicy] | None:
    if dlq_policy_conf["type"] == "produce":
        dlq_topic = dlq_policy_conf["args"][0]

        def produce_policy_creator() -> DeadLetterQueuePolicy:
            return ProduceInvalidMessagePolicy(
                KafkaProducer(build_kafka_producer_configuration(Topic(dlq_topic))),
                KafkaTopic(dlq_topic),
            )

        return produce_policy_creator
    return None


CONF_TO_PREFILTER: dict[str, Any] = {
    "kafka_header_select_filter": KafkaHeaderSelectFilter
}
CONF_TO_PROCESSOR: dict[str, Any] = {
    "generic_distributions_metrics_processor": GenericDistributionsMetricsProcessor
}


def parse_simple(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> Column[SchemaModifiers]:
    if col["type"] == "UInt":
        assert isinstance(col["args"][0], int)
        return Column(col["name"], UInt(col["args"][0], modifiers))
    elif col["type"] == "Float":
        assert isinstance(col["args"][0], int)
        return Column(col["name"], Float(col["args"][0], modifiers))
    elif col["type"] == "String":
        return Column(col["name"], String(modifiers))
    elif col["type"] == "DateTime":
        return Column(col["name"], DateTime(modifiers))
    raise


def parse_columns(columns: list[dict[str, Any]]) -> list[Column[SchemaModifiers]]:
    cols: list[Column[SchemaModifiers]] = []

    SIMPLE_COLUMN_TYPES = {
        "UInt": UInt,
        "Float": Float,
        "String": String,
        "DateTime": DateTime,
        "UUID": UUID,
    }

    for col in columns:
        modifiers = None
        if "schema_modifiers" in col:
            modifiers = SchemaModifiers(
                "nullable" in col["schema_modifiers"],
                "readonly" in col["schema_modifiers"],
            )

        column: Column[SchemaModifiers] | None = None
        if col["type"] in SIMPLE_COLUMN_TYPES:
            column = parse_simple(col, modifiers)
        elif col["type"] == "Nested":
            column = Column(col["name"], Nested(parse_columns(col["args"]), modifiers))
        elif col["type"] == "Array":
            subtype, value = col["args"]
            assert isinstance(subtype, str)
            assert isinstance(value, int)
            column = Column(
                col["name"], Array(SIMPLE_COLUMN_TYPES[subtype](value), modifiers)
            )
        elif col["type"] == "AggregateFunction":
            column = Column(
                col["name"],
                AggregateFunction(
                    col["args"][0],
                    [
                        SIMPLE_COLUMN_TYPES[c["type"]](c["arg"])
                        if "arg" in c
                        else SIMPLE_COLUMN_TYPES[c["type"]]()
                        for c in col["args"][1]
                    ],
                ),
            )
        assert column is not None
        cols.append(column)
    return cols


def deep_compare_storages(old: ReadableStorage, new: ReadableStorage) -> None:
    assert (
        old.get_mandatory_condition_checkers() == new.get_mandatory_condition_checkers()
    )
    assert old.get_query_processors() == new.get_query_processors()
    assert old.get_query_splitters() == new.get_query_splitters()
    assert (
        old.get_schema().get_columns().columns == new.get_schema().get_columns().columns
    )
