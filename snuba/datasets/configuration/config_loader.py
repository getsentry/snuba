import collections
from dataclasses import dataclass, fields
from typing import (
    Any,
    Callable,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Type,
    Union,
)

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
from snuba.datasets.storage import WritableTableStorage
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


def dataclass_from_dict(cls: Type[Any], d: Any) -> Any:
    # recursively type cast dict to classes
    # HACK: super hacks
    try:
        if cls.__origin__ == collections.abc.Sequence:
            return [dataclass_from_dict(cls.__dict__["__args__"][0], cfg) for cfg in d]
        elif cls.__origin__ == Union:
            if isinstance(d[0], str) or isinstance(d[0], int):
                return d
            elif isinstance(d[0], dict):
                for t in cls.__dict__["__args__"]:
                    result = dataclass_from_dict(t, d)
                    if not isinstance(result[0], dict):
                        return result
            return d
    except Exception:
        pass

    if cls in {str, int}:
        return d

    # https://stackoverflow.com/a/54769644
    try:
        fieldtypes = {f.name: f.type for f in fields(cls)}
        return cls(**{f: dataclass_from_dict(fieldtypes[f], d[f]) for f in d})
    except Exception:
        return d


@dataclass(frozen=True)
class StorageMetadataConfig:
    key: str
    set_key: str


@dataclass(frozen=True)
class NestedColumnConfig:
    name: str
    type: str
    args: Sequence[int]


@dataclass(frozen=True)
class ColumnConfig:
    name: str
    type: str
    args: Union[Sequence[NestedColumnConfig], Sequence[Union[str, int]]]


@dataclass(frozen=True)
class SchemaConfig:
    columns: Sequence[ColumnConfig]
    local_table_name: str
    dist_table_name: str


@dataclass(frozen=True)
class FunctionCallConfig:
    type: str
    args: Sequence[str]


@dataclass(frozen=True)
class StreamLoaderConfig:
    processor: str
    default_topic: str
    commit_log_topic: str
    subscription_scheduled_topic: str
    subscription_scheduler_mode: str
    subscription_result_topic: str
    replacement_topic: Optional[str]
    pre_filter: FunctionCallConfig
    dlq_policy: FunctionCallConfig


@dataclass(frozen=True)
class StorageConfig:
    storage: StorageMetadataConfig
    schema: SchemaConfig
    stream_loader: StreamLoaderConfig


def policy_creator_creator(
    dlq_policy_conf: FunctionCallConfig,
) -> Optional[Callable[[], DeadLetterQueuePolicy]]:
    if dlq_policy_conf.type == "produce":
        dlq_topic = dlq_policy_conf.args[0]

        def produce_policy_creator() -> DeadLetterQueuePolicy:
            return ProduceInvalidMessagePolicy(
                KafkaProducer(build_kafka_producer_configuration(Topic(dlq_topic))),
                KafkaTopic(dlq_topic),
            )

        return produce_policy_creator
    return None


CONF_TO_PREFILTER: Mapping[str, Any] = {
    "kafka_header_select_filter": KafkaHeaderSelectFilter
}
CONF_TO_PROCESSOR: Mapping[str, Any] = {
    "generic_distributions_metrics_processor": GenericDistributionsMetricsProcessor
}


def parse_simple(
    col: Union[ColumnConfig, NestedColumnConfig]
) -> Column[SchemaModifiers]:
    if col.type == "UInt":
        assert isinstance(col.args[0], int)
        return Column(col.name, UInt(col.args[0]))
    elif col.type == "Float":
        assert isinstance(col.args[0], int)
        return Column(col.name, Float(col.args[0]))
    elif col.type == "String":
        return Column(col.name, String())
    elif col.type == "DateTime":
        return Column(col.name, DateTime())
    raise


def parse_columns(
    columns: Union[Sequence[ColumnConfig], Sequence[NestedColumnConfig]]
) -> Sequence[Column[SchemaModifiers]]:
    cols: MutableSequence[Column[SchemaModifiers]] = []

    SIMPLE_COLUMN_TYPES = {
        "UInt": UInt,
        "Float": Float,
        "String": String,
        "DateTime": DateTime,
    }

    for col in columns:
        column: Optional[Column[SchemaModifiers]] = None
        if col.type in SIMPLE_COLUMN_TYPES:
            column = parse_simple(col)
        elif col.type == "Nested":
            column = Column(col.name, Nested(parse_columns(col.args)))  # type: ignore
        elif col.type == "Array":
            subtype, value = col.args
            assert isinstance(subtype, str)
            assert isinstance(value, int)
            column = Column(col.name, Array(SIMPLE_COLUMN_TYPES[subtype](value)))
        assert column is not None
        cols.append(column)
    return cols


def deep_compare_storages(old: WritableTableStorage, new: WritableTableStorage) -> None:
    assert (
        old.get_cluster().get_clickhouse_cluster_name()
        == new.get_cluster().get_clickhouse_cluster_name()
    )
    assert (
        old.get_cluster().get_storage_set_keys()
        == new.get_cluster().get_storage_set_keys()
    )
    assert old.get_is_write_error_ignorable() == new.get_is_write_error_ignorable()
    assert (
        old.get_mandatory_condition_checkers() == new.get_mandatory_condition_checkers()
    )
    assert old.get_query_processors() == new.get_query_processors()
    assert old.get_query_splitters() == new.get_query_splitters()
    assert (
        old.get_schema().get_columns().columns == new.get_schema().get_columns().columns
    )
