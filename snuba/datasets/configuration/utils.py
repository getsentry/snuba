from __future__ import annotations

from typing import Any, Callable

from arroyo import Topic as KafkaTopic
from arroyo.backends.kafka import KafkaProducer
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueuePolicy,
    ProduceInvalidMessagePolicy,
)
from jsonschema import validate
from yaml import safe_load

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
from snuba.clickhouse.processors import QueryProcessor
from snuba.datasets.configuration.json_schema import (
    READABLE_STORAGE_SCHEMA,
    WRITABLE_STORAGE_SCHEMA,
)
from snuba.datasets.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
    GenericSetsMetricsProcessor,
)
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.storages import StorageKey
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.query.processors.tuple_unaliaser import TupleUnaliaser
from snuba.utils.schemas import UUID, AggregateFunction
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


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
    return None


CONF_TO_PREFILTER: dict[str, Any] = {
    "kafka_header_select_filter": KafkaHeaderSelectFilter
}
CONF_TO_PROCESSOR: dict[str, Any] = {
    "generic_distributions_metrics_processor": GenericDistributionsMetricsProcessor,
    "generic_sets_metrics_processor": GenericSetsMetricsProcessor,
}

QUERY_PROCESSORS: dict[str, Any] = {
    "TableRateLimit": TableRateLimit,
    "TupleUnaliaser": TupleUnaliaser,
}


def get_query_processors(query_processor_names: list[str]) -> list[QueryProcessor]:
    return [QUERY_PROCESSORS[name]() for name in query_processor_names]


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


CONFIG_FILES_PATH = "./snuba/datasets/configuration/generic_metrics/storages"
CONFIG_FILES = {
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS: f"{CONFIG_FILES_PATH}/distributions.yaml",
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW: f"{CONFIG_FILES_PATH}/distributions_bucket.yaml",
    StorageKey.GENERIC_METRICS_SETS_RAW: f"{CONFIG_FILES_PATH}/sets_bucket.yaml",
}
STORAGE_VALIDATION_SCHEMAS = {
    "readonly_storage": READABLE_STORAGE_SCHEMA,
    "writable_storage": WRITABLE_STORAGE_SCHEMA,
}


def load_storage_config(storage_key: StorageKey) -> dict[str, Any]:
    file = open(CONFIG_FILES[storage_key])
    config = safe_load(file)
    assert isinstance(config, dict)
    validate(config, STORAGE_VALIDATION_SCHEMAS[config["kind"]])
    return config
