from __future__ import annotations

from typing import Any, Callable, Type, TypedDict

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
from snuba.datasets.plans.splitters import QuerySplitStrategy
from snuba.query.processors.condition_checkers import ConditionChecker
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.utils.schemas import UUID, AggregateFunction, ColumnType, IPv4, IPv6
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


class QueryProcessorDefinition(TypedDict):
    processor: str
    args: dict[str, Any]


class QuerySplitterDefinition(TypedDict):
    splitter: str
    args: dict[str, Any]


class MandatoryConditionCheckerDefinition(TypedDict):
    condition: str
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


def get_query_splitters(
    query_splitter_objects: list[QuerySplitterDefinition],
) -> list[QuerySplitStrategy]:
    return [
        QuerySplitStrategy.get_from_name(qs["splitter"]).from_kwargs(
            **qs.get("args", {})
        )
        for qs in query_splitter_objects
    ]


def get_mandatory_condition_checkers(
    mandatory_condition_checkers_objects: list[MandatoryConditionCheckerDefinition],
) -> list[ConditionChecker]:
    return [
        ConditionChecker.get_from_name(mc["condition"]).from_kwargs(
            **mc.get("args", {})
        )
        for mc in mandatory_condition_checkers_objects
    ]


NUMBER_COLUMN_TYPES: dict[str, Any] = {
    "UInt": UInt,
    "Float": Float,
}

SIMPLE_COLUMN_TYPES: dict[str, Type[ColumnType[SchemaModifiers]]] = {
    **NUMBER_COLUMN_TYPES,
    "String": String,
    "DateTime": DateTime,
    "UUID": UUID,
    "IPv4": IPv4,
    "IPv6": IPv6,
}


def __parse_simple(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> ColumnType[SchemaModifiers]:
    return SIMPLE_COLUMN_TYPES[col["type"]](modifiers)


def __parse_number(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> ColumnType[SchemaModifiers]:
    col_type = NUMBER_COLUMN_TYPES[col["type"]](col["args"]["size"], modifiers)
    assert isinstance(col_type, UInt) or isinstance(col_type, Float)
    return col_type


def __parse_column_type(col: dict[str, Any]) -> ColumnType[SchemaModifiers]:
    # TODO: Add more of Column/Value types as needed

    column_type: ColumnType[SchemaModifiers] | None = None

    modifiers: SchemaModifiers | None = None
    if "args" in col and "schema_modifiers" in col["args"]:
        modifiers = SchemaModifiers(
            "nullable" in col["args"]["schema_modifiers"],
            "readonly" in col["args"]["schema_modifiers"],
        )
    if col["type"] in NUMBER_COLUMN_TYPES:
        column_type = __parse_number(col, modifiers)
    elif col["type"] in SIMPLE_COLUMN_TYPES:
        column_type = __parse_simple(col, modifiers)
    elif col["type"] == "Nested":
        column_type = Nested(parse_columns(col["args"]["subcolumns"]), modifiers)
    elif col["type"] == "Array":
        column_type = Array(__parse_column_type(col["args"]["inner_type"]), modifiers)
    elif col["type"] == "AggregateFunction":
        column_type = AggregateFunction(
            col["args"]["func"],
            [
                SIMPLE_COLUMN_TYPES[c["type"]](c["arg"])
                if "arg" in c
                else SIMPLE_COLUMN_TYPES[c["type"]]()
                for c in col["args"]["arg_types"]
            ],
        )
    assert column_type is not None
    return column_type


def parse_columns(columns: list[dict[str, Any]]) -> list[Column[SchemaModifiers]]:
    return [Column(col["name"], __parse_column_type(col)) for col in columns]
