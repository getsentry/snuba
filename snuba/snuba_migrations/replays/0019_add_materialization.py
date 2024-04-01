from typing import Iterator, List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Array,
    Column,
    DateTime,
    IPv4,
    IPv6,
    SimpleAggregateFunction,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_iter())


def forward_iter() -> Iterator[operations.SqlOperation]:
    yield operations.CreateTable(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_aggregated_local",
        columns=columns,
        engine=table_engines.AggregatingMergeTree(
            storage_set=StorageSetKey.REPLAYS,
            order_by="(project_id, toStartOfHour(timestamp), replay_id)",
            partition_by="(retention_days, toMonday(timestamp))",
            settings={"index_granularity": "8192"},
            ttl="timestamp + toIntervalDay(retention_days)",
        ),
    )

    yield operations.AddIndex(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_aggregated_local",
        index_name="bf_replay_id",
        index_expression="replay_id",
        index_type="bloom_filter()",
        granularity=1,
        target=operations.OperationTarget.LOCAL,
    )

    yield operations.CreateMaterializedView(
        storage_set=StorageSetKey.METRICS,
        view_name="replays_aggregation_mv",
        destination_table_name="replays_aggregated_local",
        columns=columns,
        query="""
SELECT
    project_id,
    toStartOfHour(timestamp) as timestamp,
    replay_id,
    anyIfState(browser_name, browser_name != '') as browser_name,
    anyIfState(browser_version, browser_version != '') as browser_version,
    sumState(toUInt64(click_is_dead)) as count_dead_clicks,
    sumState(toUInt64(error_id != '00000000-0000-0000-0000-000000000000' OR fatal_id != '00000000-0000-0000-0000-000000000000')) as count_error_events,
    sumState(toUInt64(debug_id != '00000000-0000-0000-0000-000000000000' OR info_id != '00000000-0000-0000-0000-000000000000')) as count_info_events,
    sumState(toUInt64(click_is_rage)) as count_rage_clicks,
    countState(toUInt64(segment_id)) as count_segments,
    sumState(length(urls)) as count_urls,
    sumState(toUInt64(warning_id != '00000000-0000-0000-0000-000000000000')) as count_warning_events,
    anyIfState(device_brand, device_brand != '') as device_brand,
    anyIfState(device_family, device_family != '') as device_family,
    anyIfState(device_model, device_model != '') as device_model,
    anyIfState(device_name, device_name != '') as device_name,
    anyIfState(dist, dist != '') as dist,
    maxState(timestamp) as end,
    anyIfState(environment, environment != '') as environment,
    anyState(ip_address_v4) as ip_address_v4,
    anyState(ip_address_v6) as ip_address_v6,
    sumSimpleState(is_archived) as is_archived,
    anyIfState(os_name, os_name != '') as os_name,
    anyIfState(os_version, os_version != '') as os_version,
    anyIfState(platform, platform != '') as platform,
    anyIfState(release, release != '') as release,
    any(retention_days),
    anyIfState(sdk_name, sdk_name != '') as sdk_name,
    anyIfState(sdk_version, sdk_version != '') as sdk_version,
    minState(replay_start_timestamp) as start,
    anyIfState(user, user != '') as user,
    anyIfState(user_id, user_id != '') as user_id,
    anyIfState(user_name, user_name != '') as user_name,
    anyIfState(user_email, user_email != '') as user_email,
    groupArrayArrayState(urls) as agg_urls,
    groupArrayArrayState(arrayFilter(x -> x > 0, [error_id, fatal_id])) as error_ids,
    groupArrayArrayState(arrayFilter(x -> x > 0, [info_id, debug_id])) as info_ids,
    groupArrayState(warning_id) as warning_ids,
    groupArrayState(viewed_by_id) as viewed_by_ids,
FROM replays_local
GROUP BY project_id, toStartOfHour(timestamp), replay_id
""",
    )


def backward_iter() -> Iterator[operations.SqlOperation]:
    yield operations.DropIndex(
        StorageSetKey.REPLAYS,
        "replays_aggregated_local",
        "bf_replay_id",
        target=operations.OperationTarget.LOCAL,
    )

    yield operations.DropTable(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_aggregated_local",
    )


def AnyIfString(
    column_name: str, nullable: bool = False, low_cardinality: bool = False
) -> Column:
    return Column(
        column_name,
        AggregateFunction(
            "anyIf",
            [
                String(Modifiers(nullable=nullable, low_cardinality=low_cardinality)),
                UInt(8),
            ],
        ),
    )


def AnyIfNullableString(column_name: str, low_cardinality: bool = False) -> Column:
    """Returns an aggregate anyIf function."""
    return AnyIfString(column_name, nullable=True, low_cardinality=low_cardinality)


def AnyIfNullableLowCardinalityString(column_name: str) -> Column:
    """Returns a low-cardinality aggregate anyIf function."""
    return AnyIfString(column_name, nullable=True, low_cardinality=True)


def Sum(column_name: str) -> Column:
    """Returns an aggregate sum function."""
    return Column(column_name, AggregateFunction("sum", [UInt(64)]))


def CountNullable(column_name: str) -> Column:
    """Returns an aggregate count function capable of accepting nullable integer values."""
    return Column(
        column_name, AggregateFunction("count", [UInt(64, Modifiers(nullable=True))])
    )


columns: List[Column[Modifiers]] = [
    # Primary-key.
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("replay_id", UUID()),
    # Columns ordered by column-name.
    Column("agg_urls", AggregateFunction("groupArrayArray", [Array(String)])),
    AnyIfNullableLowCardinalityString("browser_name"),
    AnyIfNullableString("browser_version"),
    Sum("count_dead_clicks"),
    Sum("count_error_events"),
    Sum("count_info_events"),
    Sum("count_rage_clicks"),
    CountNullable("count_segments"),
    Sum("count_urls"),
    Sum("count_warning_events"),
    AnyIfNullableLowCardinalityString("device_brand"),
    AnyIfNullableLowCardinalityString("device_family"),
    AnyIfNullableLowCardinalityString("device_model"),
    AnyIfNullableLowCardinalityString("device_name"),
    AnyIfNullableString("dist"),
    Column("end", AggregateFunction("max", [DateTime()])),
    AnyIfNullableLowCardinalityString("environment"),
    Column("error_ids", AggregateFunction("groupArrayArray", [Array(UUID)])),
    Column("info_ids", AggregateFunction("groupArrayArray", [Array(UUID)])),
    Column("ip_address_v4", AggregateFunction("any", [IPv4(Modifiers(nullable=True))])),
    Column("ip_address_v6", AggregateFunction("any", [IPv6(Modifiers(nullable=True))])),
    Column(
        "is_archived",
        SimpleAggregateFunction("sum", [UInt(64, Modifiers(nullable=True))]),
    ),
    AnyIfNullableLowCardinalityString("os_name"),
    AnyIfNullableString("os_version"),
    AnyIfString("platform", low_cardinality=True),
    AnyIfNullableString("release"),
    Column("retention_days", UInt(16)),
    AnyIfNullableLowCardinalityString("sdk_name"),
    AnyIfNullableLowCardinalityString("sdk_version"),
    Column("start", AggregateFunction("min", [DateTime(Modifiers(nullable=True))])),
    AnyIfNullableString("user"),
    AnyIfNullableString("user_id"),
    AnyIfNullableString("user_name"),
    AnyIfNullableString("user_email"),
    Column("warning_ids", AggregateFunction("groupArray", [UUID])),
    Column("viewed_by_ids", AggregateFunction("groupArray", [UUID])),
]
