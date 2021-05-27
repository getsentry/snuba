from typing import Sequence

from snuba.clickhouse.columns import Array, Column, DateTime, Nested, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import INT_TAGS_HASH_MAP_COLUMN
from snuba.migrations import operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

PRE_VALUE_BUCKETS_COLUMNS: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))])),
]

POST_VALUES_BUCKETS_COLUMNS: Sequence[Column[Modifiers]] = [
    Column("materialization_version", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


def get_forward_bucket_table_local(
    table_name: str, value_cols: Sequence[Column[Modifiers]],
) -> Sequence[operations.SqlOperation]:
    return [
        operations.CreateTable(
            storage_set=StorageSetKey.METRICS,
            table_name=table_name,
            columns=[
                *PRE_VALUE_BUCKETS_COLUMNS,
                *value_cols,
                *POST_VALUES_BUCKETS_COLUMNS,
            ],
            engine=table_engines.MergeTree(
                storage_set=StorageSetKey.METRICS,
                order_by="(org_id, project_id, metric_id, timestamp)",
                partition_by="toMonday(timestamp)",
                ttl="timestamp + toIntervalDay(14)",
            ),
        )
    ]


def get_forward_bucket_table_dist(
    local_table_name: str,
    dist_table_name: str,
    value_cols: Sequence[Column[Modifiers]],
) -> Sequence[operations.SqlOperation]:
    return [
        operations.CreateTable(
            storage_set=StorageSetKey.METRICS,
            table_name=dist_table_name,
            columns=[
                *PRE_VALUE_BUCKETS_COLUMNS,
                *value_cols,
                *POST_VALUES_BUCKETS_COLUMNS,
            ],
            engine=table_engines.Distributed(
                local_table_name=local_table_name, sharding_key=None,
            ),
        ),
    ]


COMMON_AGGR_COLUMNS: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("granularity", UInt(32)),
    Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))])),
    Column("timestamp", DateTime()),
    Column("retention_days", UInt(16)),
]


MATVIEW_STATEMENT = """
SELECT
    org_id,
    project_id,
    metric_id,
    60 as granularity,
    tags.key,
    tags.value,
    toStartOfMinute(timestamp) as timestamp,
    retention_days,
    %(aggregation_states)s
FROM metrics_buckets_local
WHERE materialization_version = 0
GROUP BY
    org_id,
    project_id,
    metric_id,
    tags.key,
    tags.value,
    timestamp,
    granularity,
    retention_days
"""


def get_forward_migrations_local(
    table_name: str,
    mv_name: str,
    aggregation_col_schema: Sequence[Column[Modifiers]],
    aggregation_states: str,
) -> Sequence[operations.SqlOperation]:
    aggregated_cols = [*COMMON_AGGR_COLUMNS, *aggregation_col_schema]
    return [
        operations.CreateTable(
            storage_set=StorageSetKey.METRICS,
            table_name=table_name,
            columns=aggregated_cols,
            engine=table_engines.AggregatingMergeTree(
                storage_set=StorageSetKey.METRICS,
                order_by="(org_id, project_id, metric_id, granularity, timestamp, tags.key, tags.value)",
                partition_by="(retention_days, toMonday(timestamp))",
                settings={"index_granularity": "256"},
            ),
        ),
        operations.AddColumn(
            storage_set=StorageSetKey.METRICS,
            table_name=table_name,
            column=Column(
                "_tags_hash",
                Array(UInt(64), Modifiers(materialized=INT_TAGS_HASH_MAP_COLUMN)),
            ),
            after="tags.value",
        ),
        operations.AddIndex(
            storage_set=StorageSetKey.METRICS,
            table_name=table_name,
            index_name="bf_tags_hash",
            index_expression="_tags_hash",
            index_type="bloom_filter()",
            granularity=1,
        ),
        operations.AddIndex(
            storage_set=StorageSetKey.METRICS,
            table_name=table_name,
            index_name="bf_tags_key_hash",
            index_expression="tags.key",
            index_type="bloom_filter()",
            granularity=1,
        ),
        operations.CreateMaterializedView(
            storage_set=StorageSetKey.METRICS,
            view_name=mv_name,
            destination_table_name=table_name,
            columns=aggregated_cols,
            query=MATVIEW_STATEMENT % {"aggregation_states": aggregation_states},
        ),
    ]


def get_forward_migrations_dist(
    dist_table_name: str,
    local_table_name: str,
    aggregation_col_schema: Sequence[Column[Modifiers]],
) -> Sequence[operations.SqlOperation]:
    return [
        operations.CreateTable(
            storage_set=StorageSetKey.METRICS,
            table_name=dist_table_name,
            columns=[*COMMON_AGGR_COLUMNS, *aggregation_col_schema],
            engine=table_engines.Distributed(
                local_table_name=local_table_name, sharding_key=None
            ),
        ),
        operations.AddColumn(
            storage_set=StorageSetKey.METRICS,
            table_name=dist_table_name,
            column=Column(
                "_tags_hash",
                Array(UInt(64), Modifiers(materialized=INT_TAGS_HASH_MAP_COLUMN)),
            ),
            after="tags.value",
        ),
    ]


def get_reverse_table_migration(table_name: str) -> Sequence[operations.SqlOperation]:
    return [
        operations.DropTable(storage_set=StorageSetKey.METRICS, table_name=table_name,),
    ]
