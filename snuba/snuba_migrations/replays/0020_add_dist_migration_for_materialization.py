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
        table_name="replays_aggregated_dist",
        columns=columns,
        engine=table_engines.Distributed(
            local_table_name="replays_aggregated_local",
            sharding_key=None,
        ),
        target=operations.OperationTarget.DISTRIBUTED,
    )


def backward_iter() -> Iterator[operations.SqlOperation]:
    yield operations.DropTable(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_aggregated_dist",
        target=operations.OperationTarget.DISTRIBUTED,
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
