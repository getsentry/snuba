from typing import Iterator, List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Column,
    DateTime,
    IPv4,
    IPv6,
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
            order_by="(project_id, to_hour_timestamp, replay_id, retention_days)",
            partition_by="(retention_days, toMonday(to_hour_timestamp))",
            settings={"index_granularity": "8192"},
            ttl="to_hour_timestamp + toIntervalDay(retention_days)",
        ),
        target=operations.OperationTarget.LOCAL,
    )

    yield operations.CreateMaterializedView(
        storage_set=StorageSetKey.REPLAYS,
        view_name="replays_aggregation_mv",
        destination_table_name="replays_aggregated_local",
        columns=columns,
        query="""
SELECT
    project_id,
    toStartOfHour(timestamp) as to_hour_timestamp,
    replay_id,
    retention_days,
    anyIfState(browser_name, browser_name != '') as browser_name,
    anyIfState(browser_version, browser_version != '') as browser_version,
    sumState(toUInt64(click_is_dead)) as count_dead_clicks,
    sumState(toUInt64(error_id != '00000000-0000-0000-0000-000000000000' OR fatal_id != '00000000-0000-0000-0000-000000000000')) as count_errors,
    sumState(toUInt64(debug_id != '00000000-0000-0000-0000-000000000000' OR info_id != '00000000-0000-0000-0000-000000000000')) as count_infos,
    sumState(toUInt64(click_is_rage)) as count_rage_clicks,
    countState(toUInt64(segment_id)) as count_segments,
    sumState(length(urls)) as count_urls,
    sumState(toUInt64(warning_id != '00000000-0000-0000-0000-000000000000')) as count_warnings,
    anyIfState(device_brand, device_brand != '') as device_brand,
    anyIfState(device_family, device_family != '') as device_family,
    anyIfState(device_model, device_model != '') as device_model,
    anyIfState(device_name, device_name != '') as device_name,
    anyIfState(dist, dist != '') as dist,
    maxIfState(timestamp, segment_id IS NOT NULL) as finished_at,
    anyIfState(environment, environment != '') as environment,
    anyState(ip_address_v4) as ip_address_v4,
    anyState(ip_address_v6) as ip_address_v6,
    sumState(toUInt64(is_archived)) as is_archived,
    anyIfState(os_name, os_name != '') as os_name,
    anyIfState(os_version, os_version != '') as os_version,
    anyIfState(platform, platform != '') as platform,
    anyIfState(sdk_name, sdk_name != '') as sdk_name,
    anyIfState(sdk_version, sdk_version != '') as sdk_version,
    minState(replay_start_timestamp) as started_at,
    anyIfState(user, user != '') as user,
    anyIfState(user_id, user_id != '') as user_id,
    anyIfState(user_name, user_name != '') as user_name,
    anyIfState(user_email, user_email != '') as user_email,
    minState(segment_id) as min_segment_id
FROM replays_local
GROUP BY project_id, toStartOfHour(timestamp), replay_id, retention_days
""",
        target=operations.OperationTarget.LOCAL,
    )


def backward_iter() -> Iterator[operations.SqlOperation]:
    yield operations.DropTable(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_aggregation_mv",
        target=operations.OperationTarget.LOCAL,
    )

    yield operations.DropTable(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_aggregated_local",
        target=operations.OperationTarget.LOCAL,
    )


def any_if_string(
    column_name: str, nullable: bool = False, low_cardinality: bool = False
) -> Column[Modifiers]:
    return Column(
        column_name,
        AggregateFunction(
            "anyIf",
            [
                String(Modifiers(nullable=nullable, low_cardinality=low_cardinality)),
                UInt(8, Modifiers(nullable=nullable)),
            ],
        ),
    )


def any_if_nullable_string(
    column_name: str, low_cardinality: bool = False
) -> Column[Modifiers]:
    """Returns an aggregate anyIf function."""
    return any_if_string(column_name, nullable=True, low_cardinality=low_cardinality)


def sum(column_name: str) -> Column[Modifiers]:
    """Returns an aggregate sum function."""
    return Column(column_name, AggregateFunction("sum", [UInt(64)]))


def count_nullable(column_name: str) -> Column[Modifiers]:
    """Returns an aggregate count function capable of accepting nullable integer values."""
    return Column(
        column_name, AggregateFunction("count", [UInt(64, Modifiers(nullable=True))])
    )


columns: List[Column[Modifiers]] = [
    # Primary-key.
    Column("project_id", UInt(64)),
    Column("to_hour_timestamp", DateTime()),
    Column("replay_id", UUID()),
    Column("retention_days", UInt(16)),
    # Columns ordered by column-name.
    any_if_nullable_string("browser_name"),
    any_if_nullable_string("browser_version"),
    sum("count_dead_clicks"),
    sum("count_errors"),
    sum("count_infos"),
    sum("count_rage_clicks"),
    count_nullable("count_segments"),
    sum("count_urls"),
    sum("count_warnings"),
    any_if_nullable_string("device_brand"),
    any_if_nullable_string("device_family"),
    any_if_nullable_string("device_model"),
    any_if_nullable_string("device_name"),
    any_if_nullable_string("dist"),
    any_if_nullable_string("environment"),
    Column("finished_at", AggregateFunction("maxIf", [DateTime(), UInt(8)])),
    Column("ip_address_v4", AggregateFunction("any", [IPv4(Modifiers(nullable=True))])),
    Column("ip_address_v6", AggregateFunction("any", [IPv6(Modifiers(nullable=True))])),
    Column(
        "is_archived", AggregateFunction("sum", [UInt(64, Modifiers(nullable=True))])
    ),
    Column(
        "min_segment_id", AggregateFunction("min", [UInt(16, Modifiers(nullable=True))])
    ),
    any_if_nullable_string("os_name"),
    any_if_nullable_string("os_version"),
    any_if_string("platform", low_cardinality=False),
    any_if_nullable_string("sdk_name"),
    any_if_nullable_string("sdk_version"),
    Column(
        "started_at", AggregateFunction("min", [DateTime(Modifiers(nullable=True))])
    ),
    any_if_nullable_string("user"),
    any_if_nullable_string("user_id"),
    any_if_nullable_string("user_name"),
    any_if_nullable_string("user_email"),
]
