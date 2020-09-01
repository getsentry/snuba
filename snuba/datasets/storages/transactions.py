from datetime import datetime
from typing import Mapping, Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    ColumnType,
    DateTime,
    Float,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nested,
    Nullable,
    String,
    UInt,
    WithDefault,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.datasets.storages.transaction_column_processor import (
    TransactionColumnProcessor,
)
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.datasets.transactions_processor import (
    UNKNOWN_SPAN_STATUS,
    TransactionsMessageProcessor,
)
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.query.processors.replace_expressions import transform_user_to_nullable
from snuba.query.processors.tagsmap import NestedFieldConditionOptimizer
from snuba.web.split import TimeSplitQueryStrategy

# This is the moment in time we started filling in flattened_tags and flattened_contexts
# columns. It is captured to use the flattened tags optimization only for queries that
# do not go back this much in time.
# Will be removed in february.
BEGINNING_OF_TIME = datetime(2019, 12, 11, 0, 0, 0)


def transactions_migrations(
    clickhouse_table: str, current_schema: Mapping[str, ColumnType]
) -> Sequence[str]:
    ret = []
    duration_col = current_schema.get("duration")
    if duration_col and Materialized in duration_col.get_all_modifiers():
        ret.append("ALTER TABLE %s MODIFY COLUMN duration UInt32" % clickhouse_table)

    if "sdk_name" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN sdk_name LowCardinality(String) DEFAULT ''"
            % clickhouse_table
        )

    if "sdk_version" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN sdk_version LowCardinality(String) DEFAULT ''"
            % clickhouse_table
        )

    if "transaction_status" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN transaction_status UInt8 DEFAULT {UNKNOWN_SPAN_STATUS} AFTER transaction_op"
        )

    if "_tags_flattened" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN _tags_flattened String DEFAULT ''"
        )

    if "_contexts_flattened" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN _contexts_flattened String DEFAULT ''"
        )

    if "user_hash" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN user_hash UInt64 MATERIALIZED cityHash64(user) AFTER user"
        )

    low_cardinality_cols = [
        "transaction_name",
        "release",
        "dist",
        "sdk_name",
        "sdk_version",
        "environment",
    ]
    for col_name in low_cardinality_cols:
        col = current_schema.get(col_name)

        if col and LowCardinality not in col.get_all_modifiers():
            if isinstance(col, WithDefault):
                col.inner_type = LowCardinality(col.inner_type)
            else:
                col = LowCardinality(col)
            ret.append(
                f"ALTER TABLE {clickhouse_table} MODIFY COLUMN {col_name} {col.for_schema()}"
            )

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

    # `Nested` is only syntactic sugar for table creation. Nested columns are actually arrays.
    # So current_schema does not contain any single `measurements` column. It includes
    # two separate arrays instead.
    if "measurements.key" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN measurements.key Array(LowCardinality(String)) "
            f"AFTER _contexts_flattened"
        )

    if "measurements.value" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN measurements.value Array(Float64) "
            f"AFTER measurements.key"
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


columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("event_id", UUID()),
        ("trace_id", UUID()),
        ("span_id", UInt(64)),
        ("transaction_name", LowCardinality(String())),
        ("transaction_hash", Materialized(UInt(64), "cityHash64(transaction_name)",),),
        ("transaction_op", LowCardinality(String())),
        ("transaction_status", WithDefault(UInt(8), str(UNKNOWN_SPAN_STATUS))),
        ("start_ts", DateTime()),
        ("start_ms", UInt(16)),
        ("finish_ts", DateTime()),
        ("finish_ms", UInt(16)),
        ("duration", UInt(32)),
        ("platform", LowCardinality(String())),
        ("environment", LowCardinality(Nullable(String()))),
        ("release", LowCardinality(Nullable(String()))),
        ("dist", LowCardinality(Nullable(String()))),
        ("ip_address_v4", Nullable(IPv4())),
        ("ip_address_v6", Nullable(IPv6())),
        ("user", WithDefault(String(), "''",)),
        ("user_hash", Materialized(UInt(64), "cityHash64(user)"),),
        ("user_id", Nullable(String())),
        ("user_name", Nullable(String())),
        ("user_email", Nullable(String())),
        ("sdk_name", WithDefault(LowCardinality(String()), "''")),
        ("sdk_version", WithDefault(LowCardinality(String()), "''")),
        ("http_method", LowCardinality(Nullable(String()))),
        ("http_referer", Nullable(String())),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_flattened", String()),
        ("_tags_hash_map", Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN)),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("_contexts_flattened", String()),
        (
            "measurements",
            Nested([("key", LowCardinality(String())), ("value", Float(64))]),
        ),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = ReplacingMergeTreeSchema(
    columns=columns,
    local_table_name="transactions_local",
    dist_table_name="transactions_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
    mandatory_conditions=[],
    prewhere_candidates=["event_id", "transaction_name", "transaction", "title"],
    order_by="(project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id))",
    partition_by="(retention_days, toMonday(finish_ts))",
    version_column="deleted",
    sample_expr="cityHash64(span_id)",
    ttl_expr="finish_ts + toIntervalDay(retention_days)",
    settings={"index_granularity": "8192"},
    migration_function=transactions_migrations,
    # Tags hashmap is a materialized column. Clickhouse does not allow
    # us to create a materialized column that references a nested one
    # during create statement
    # (https://github.com/ClickHouse/ClickHouse/issues/12586), so the
    # materialization is added with a migration.
    skipped_cols_on_creation={"_tags_hash_map"},
)


storage = WritableTableStorage(
    storage_key=StorageKey.TRANSACTIONS,
    storage_set_key=StorageSetKey.TRANSACTIONS,
    schema=schema,
    query_processors=[
        NestedFieldConditionOptimizer(
            "contexts",
            "_contexts_flattened",
            {"start_ts", "finish_ts"},
            BEGINNING_OF_TIME,
        ),
        MappingOptimizer("tags", "_tags_hash_map"),
        TransactionColumnProcessor(),
        ArrayJoinKeyValueOptimizer("tags"),
        PrewhereProcessor(),
        transform_user_to_nullable(),
    ],
    stream_loader=KafkaStreamLoader(
        processor=TransactionsMessageProcessor(), default_topic="events",
    ),
    query_splitters=[TimeSplitQueryStrategy(timestamp_col="finish_ts")],
    writer_options={"insert_allow_materialized_columns": 1},
)
