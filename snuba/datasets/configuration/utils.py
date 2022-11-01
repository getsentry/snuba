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
from snuba.datasets.plans.splitters import QuerySplitStrategy
from snuba.datasets.storage import ReadableTableStorage
from snuba.query.processors.condition_checkers import ConditionChecker
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.utils.schemas import UUID, AggregateFunction, IPv4, IPv6
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


NUMBER_COLUMN_TYPES = {"UInt": UInt, "Float": Float}

SIMPLE_COLUMN_TYPES = {
    **NUMBER_COLUMN_TYPES,
    "String": String,
    "DateTime": DateTime,
    "UUID": UUID,
    "IPv4": IPv4,
    "IPv6": IPv6,
}


def __parse_simple(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> Column[SchemaModifiers]:
    return Column(col["name"], SIMPLE_COLUMN_TYPES[col["type"]](modifiers))


def __parse_number(
    col: dict[str, Any], modifiers: SchemaModifiers | None
) -> Column[SchemaModifiers]:
    return Column(
        col["name"], NUMBER_COLUMN_TYPES[col["type"]](col["args"]["size"], modifiers)
    )


def parse_columns(columns: list[dict[str, Any]]) -> list[Column[SchemaModifiers]]:
    # TODO: Add more of Column/Value types as needed

    cols: list[Column[SchemaModifiers]] = []

    for col in columns:
        column: Column[SchemaModifiers] | None = None
        modifiers: SchemaModifiers | None = None
        if "args" in col and "schema_modifiers" in col["args"]:
            modifiers = SchemaModifiers(
                "nullable" in col["args"]["schema_modifiers"],
                "readonly" in col["args"]["schema_modifiers"],
            )
        if col["type"] in NUMBER_COLUMN_TYPES:
            column = __parse_number(col, modifiers)
        elif col["type"] in SIMPLE_COLUMN_TYPES:
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


def serialize_columns(storage: ReadableTableStorage) -> list[dict[str, Any]]:
    cols: list[dict[str, Any]] = []
    for col in storage.get_schema().get_columns().columns:
        args: dict[str, Any] = {}
        for key, val in col.type.__dict__.items():
            if key == "nested_columns":
                continue
            elif key == "inner_type":
                args["type"] = val.__class__.__name__
                args["arg"] = val.__dict__["size"]
                if modifiers := col.type.get_modifiers():
                    args["schema_modifiers"] = [
                        modifier
                        for modifier, exists in {
                            "readonly": modifiers.readonly,
                            "nullable": modifiers.nullable,
                        }.items()
                        if exists
                    ]
            elif key == "arg_types":
                continue
            elif key == "_ColumnType__modifiers":
                continue
            else:
                args[key] = val

        cols.append(
            {
                "name": col.name,
                "type": col.type.__class__.__name__,
                "args": args,
            }
        )
    return cols
