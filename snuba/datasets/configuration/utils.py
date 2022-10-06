from __future__ import annotations

from typing import Any, Callable, TypedDict

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
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.utils.schemas import UUID, AggregateFunction
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


class QueryProcessorDefinition(TypedDict):
    processor: str
    args: dict[str, Any]


def generate_policy_creator(
    dlq_policy_config: dict[str, Any],
) -> Callable[[], DeadLetterQueuePolicy] | None:
    """
    Creates a DLQ Policy creator function.
    """
    if dlq_policy_config["type"] == "produce":
        dlq_topic = dlq_policy_config["args"][0]

        def produce_policy_creator() -> DeadLetterQueuePolicy:
            return ProduceInvalidMessagePolicy(
                KafkaProducer(build_kafka_producer_configuration(Topic(dlq_topic))),
                KafkaTopic(dlq_topic),
            )

        return produce_policy_creator
    # TODO: Add rest of DLQ policy types
    return None


def get_query_processors(
    query_processor_objects: list[QueryProcessorDefinition],
) -> list[ClickhouseQueryProcessor]:
    return [
        ClickhouseQueryProcessor.get_from_name(qp["processor"]).from_kwargs(
            **qp.get("args", {})
        )
        for qp in query_processor_objects
    ]


def __parse_simple(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> Column[SchemaModifiers]:
    if col["type"] == "UInt":
        return Column(col["name"], UInt(col["args"]["size"], modifiers))
    elif col["type"] == "Float":
        return Column(col["name"], Float(col["args"]["size"], modifiers))
    elif col["type"] == "String":
        return Column(col["name"], String(modifiers))
    elif col["type"] == "DateTime":
        return Column(col["name"], DateTime(modifiers))
    elif col["type"] == "UUID":
        return Column(col["name"], UUID(modifiers))
    raise


def parse_columns(columns: list[dict[str, Any]]) -> list[Column[SchemaModifiers]]:
    # TODO: Add more of Column/Value types as needed

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
        if "args" in col and "schema_modifiers" in col["args"]:
            modifiers = SchemaModifiers(
                "nullable" in col["args"]["schema_modifiers"],
                "readonly" in col["args"]["schema_modifiers"],
            )

        column: Column[SchemaModifiers] | None = None
        if col["type"] in SIMPLE_COLUMN_TYPES:
            column = __parse_simple(col, modifiers)
        elif col["type"] == "Nested":
            column = Column(
                col["name"], Nested(parse_columns(col["args"]["subcolumns"]), modifiers)
            )
        elif col["type"] == "Array":
            column = Column(
                col["name"],
                Array(
                    SIMPLE_COLUMN_TYPES[col["args"]["type"]](col["args"]["arg"]),
                    modifiers,
                ),
            )
        elif col["type"] == "AggregateFunction":
            column = Column(
                col["name"],
                AggregateFunction(
                    col["args"]["func"],
                    [
                        SIMPLE_COLUMN_TYPES[c["type"]](c["arg"])
                        if "arg" in c
                        else SIMPLE_COLUMN_TYPES[c["type"]]()
                        for c in col["args"]["arg_types"]
                    ],
                ),
            )
        assert column is not None
        cols.append(column)
    return cols
