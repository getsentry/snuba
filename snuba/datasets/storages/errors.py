from typing import Mapping, Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    ColumnType,
    DateTime,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nested,
    Nullable,
    String,
    UInt,
    WithCodecs,
    WithDefault,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.schemas import MandatoryCondition
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor


def errors_migrations(
    clickhouse_table: str, current_schema: Mapping[str, ColumnType]
) -> Sequence[str]:
    ret = []

    if "message_timestamp" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN message_timestamp DateTime AFTER offset"
        )

    if "_tags_hash_map" not in current_schema:
        ret.append(
            (
                f"ALTER TABLE {clickhouse_table} ADD COLUMN _tags_hash_map Array(UInt64) "
                f"MATERIALIZED {TAGS_HASH_MAP_COLUMN} AFTER _tags_flattened"
            )
        )

    if "http_method" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN http_method LowCardinality(Nullable(String)) AFTER sdk_version"
        )

    if "http_referer" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN http_referer Nullable(String) AFTER http_method"
        )

    return ret


all_columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("timestamp", DateTime()),
        ("event_id", WithCodecs(UUID(), ["NONE"])),
        ("platform", LowCardinality(String())),
        ("environment", LowCardinality(Nullable(String()))),
        ("release", LowCardinality(Nullable(String()))),
        ("dist", LowCardinality(Nullable(String()))),
        ("ip_address_v4", Nullable(IPv4())),
        ("ip_address_v6", Nullable(IPv6())),
        ("user", WithDefault(String(), "''")),
        ("user_hash", Materialized(UInt(64), "cityHash64(user)"),),
        ("user_id", Nullable(String())),
        ("user_name", Nullable(String())),
        ("user_email", Nullable(String())),
        ("sdk_name", LowCardinality(Nullable(String()))),
        ("sdk_version", LowCardinality(Nullable(String()))),
        ("http_method", LowCardinality(Nullable(String()))),
        ("http_referer", Nullable(String())),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN)),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("transaction_name", WithDefault(LowCardinality(String()), "''")),
        ("transaction_hash", Materialized(UInt(64), "cityHash64(transaction_name)"),),
        ("span_id", Nullable(UInt(64))),
        ("trace_id", Nullable(UUID())),
        ("partition", UInt(16)),
        ("offset", WithCodecs(UInt(64), ["DoubleDelta", "LZ4"])),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
        ("group_id", UInt(64)),
        ("primary_hash", UUID()),
        ("received", DateTime()),
        ("message", String()),
        ("title", String()),
        ("culprit", String()),
        ("level", LowCardinality(String())),
        ("location", Nullable(String())),
        ("version", LowCardinality(Nullable(String()))),
        ("type", LowCardinality(String())),
        (
            "exception_stacks",
            Nested(
                [
                    ("type", Nullable(String())),
                    ("value", Nullable(String())),
                    ("mechanism_type", Nullable(String())),
                    ("mechanism_handled", Nullable(UInt(8))),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", Nullable(String())),
                    ("colno", Nullable(UInt(32))),
                    ("filename", Nullable(String())),
                    ("function", Nullable(String())),
                    ("lineno", Nullable(UInt(32))),
                    ("in_app", Nullable(UInt(8))),
                    ("package", Nullable(String())),
                    ("module", Nullable(String())),
                    ("stack_level", Nullable(UInt(16))),
                ]
            ),
        ),
        ("sdk_integrations", Array(String())),
        ("modules", Nested([("name", String()), ("version", String())])),
    ]
)

promoted_tag_columns = {
    "environment": "environment",
    "sentry:release": "release",
    "sentry:dist": "dist",
    "sentry:user": "user",
    "transaction": "transaction_name",
    "level": "level",
}

schema = WritableTableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=[
        MandatoryCondition(
            ("deleted", "=", 0),
            binary_condition(
                None,
                ConditionFunctions.EQ,
                Column(None, None, "deleted"),
                Literal(None, 0),
            ),
        )
    ],
    prewhere_candidates=[
        "event_id",
        "group_id",
        "tags[sentry:release]",
        "message",
        "environment",
        "project_id",
    ],
)

required_columns = [
    "event_id",
    "project_id",
    "group_id",
    "timestamp",
    "deleted",
    "retention_days",
]

storage = WritableTableStorage(
    storage_key=StorageKey.ERRORS,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=[
        PostReplacementConsistencyEnforcer(
            project_column="project_id", replacer_state_name=ReplacerState.ERRORS,
        ),
        MappingColumnPromoter(mapping_specs={"tags": promoted_tag_columns}),
        ArrayJoinKeyValueOptimizer("tags"),
        PrewhereProcessor(),
    ],
    stream_loader=KafkaStreamLoader(
        processor=ErrorsProcessor(promoted_tag_columns),
        default_topic="events",
        replacement_topic="errors-replacements",
    ),
    replacer_processor=ErrorsReplacer(
        write_schema=schema,
        read_schema=schema,
        required_columns=required_columns,
        tag_column_map={"tags": promoted_tag_columns, "contexts": {}},
        promoted_tags={"tags": list(promoted_tag_columns.keys()), "contexts": []},
        state_name=ReplacerState.ERRORS,
    ),
)
