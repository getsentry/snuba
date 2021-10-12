from typing import Sequence, TypedDict

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    DateTime,
    Float,
    Nested,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import INT_TAGS_HASH_MAP_COLUMN
from snuba.migrations import operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

#: The granularity used for the initial materialized views.
#: This might differ from snuba.datasets.metrics.DEFAULT_GRANULARITY at
#: a later point.
ORIGINAL_GRANULARITY = 60

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

COL_SCHEMA_DISTRIBUTIONS: Sequence[Column[Modifiers]] = [
    Column(
        "percentiles",
        AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
    ),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column("count", AggregateFunction("count", [Float(64)])),
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
    %(granularity)d as granularity,
    tags.key,
    tags.value,
    toStartOfInterval(timestamp, INTERVAL %(granularity)d second) as timestamp,
    retention_days,
    %(aggregation_states)s
FROM %(raw_table_name)s
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
    source_table_name: str,
    table_name: str,
    mv_name: str,
    aggregation_col_schema: Sequence[Column[Modifiers]],
    aggregation_states: str,
    granularity: int,
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
    ] + [
        get_forward_view_migration_local(
            source_table_name,
            table_name,
            mv_name,
            aggregation_col_schema,
            aggregation_states,
            granularity,
        )
    ]


def get_forward_view_migration_local(
    source_table_name: str,
    table_name: str,
    mv_name: str,
    aggregation_col_schema: Sequence[Column[Modifiers]],
    aggregation_states: str,
    granularity: int,
) -> operations.SqlOperation:
    aggregated_cols = [*COMMON_AGGR_COLUMNS, *aggregation_col_schema]
    return operations.CreateMaterializedView(
        storage_set=StorageSetKey.METRICS,
        view_name=mv_name,
        destination_table_name=table_name,
        columns=aggregated_cols,
        query=MATVIEW_STATEMENT
        % {
            "raw_table_name": source_table_name,
            "aggregation_states": aggregation_states,
            "granularity": granularity,
        },
    )


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


def get_mv_name(metric_type: str, granularity: int) -> str:
    if granularity == ORIGINAL_GRANULARITY:
        return f"metrics_{metric_type}_mv_local"

    return f"metrics_{metric_type}_mv_{granularity}s_local"


class MigrationArgs(TypedDict):
    source_table_name: str
    table_name: str
    mv_name: str
    aggregation_col_schema: Sequence[Column[Modifiers]]
    aggregation_states: str
    granularity: int


def get_migration_args_for_sets(
    granularity: int = ORIGINAL_GRANULARITY,
) -> MigrationArgs:
    return {
        "source_table_name": "metrics_buckets_local",
        "table_name": "metrics_sets_local",
        "mv_name": get_mv_name("sets", granularity),
        "aggregation_col_schema": [
            Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
        ],
        "aggregation_states": "uniqCombined64State(arrayJoin(set_values)) as value",
        "granularity": granularity,
    }


def get_migration_args_for_counters(
    granularity: int = ORIGINAL_GRANULARITY,
) -> MigrationArgs:
    return {
        "source_table_name": "metrics_counters_buckets_local",
        "table_name": "metrics_counters_local",
        "mv_name": get_mv_name("counters", granularity),
        "aggregation_col_schema": [
            Column("value", AggregateFunction("sum", [Float(64)])),
        ],
        "aggregation_states": "sumState(value) as value",
        "granularity": granularity,
    }


def get_migration_args_for_distributions(
    granularity: int = ORIGINAL_GRANULARITY,
) -> MigrationArgs:
    return {
        "source_table_name": "metrics_distributions_buckets_local",
        "table_name": "metrics_distributions_local",
        "mv_name": get_mv_name("distributions", granularity),
        "aggregation_col_schema": COL_SCHEMA_DISTRIBUTIONS,
        "aggregation_states": (
            "quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)((arrayJoin(values) AS values_rows)) as percentiles, "
            "minState(values_rows) as min, "
            "maxState(values_rows) as max, "
            "avgState(values_rows) as avg, "
            "sumState(values_rows) as sum, "
            "countState(values_rows) as count"
        ),
        "granularity": granularity,
    }
