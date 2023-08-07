from typing import Callable, Iterator, List, Sequence, Tuple, Type

from snuba.clickhouse.columns import Column, ColumnType, Float, IPv4, IPv6, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.ModifyColumn]:
    for column_name, column_type in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(column_name, column_type(False)),
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(column_name, column_type(False)),
            target=operations.OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column_name, column_type in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(column_name, column_type(True)),
            target=operations.OperationTarget.DISTRIBUTED,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(column_name, column_type(True)),
            target=operations.OperationTarget.LOCAL,
        )


def column_modifier(
    column_type: Type[ColumnType[Modifiers]], nullable: bool
) -> ColumnType[Modifiers]:
    return column_type(Modifiers(nullable=nullable))


def f64_modifier(nullable: bool) -> ColumnType[Modifiers]:
    return Float(64, Modifiers(default="0.0", nullable=nullable))


def ipv4_modifier(nullable: bool) -> ColumnType[Modifiers]:
    return IPv4(Modifiers(nullable=nullable))


def ipv6_modifier(nullable: bool) -> ColumnType[Modifiers]:
    return IPv6(Modifiers(nullable=nullable))


def string_modifier(nullable: bool) -> ColumnType[Modifiers]:
    return String(Modifiers(nullable=nullable))


def uint8_modifier(nullable: bool) -> ColumnType[Modifiers]:
    return UInt(8, Modifiers(nullable=nullable))


columns: List[Tuple[str, Callable[[bool], ColumnType[Modifiers]]]] = [
    ("browser_name", string_modifier),
    ("browser_version", string_modifier),
    ("device_brand", string_modifier),
    ("device_family", string_modifier),
    ("device_model", string_modifier),
    ("device_name", string_modifier),
    ("dist", string_modifier),
    ("environment", string_modifier),
    ("error_sample_rate", f64_modifier),
    ("ip_address_v4", ipv4_modifier),
    ("ip_address_v6", ipv6_modifier),
    ("is_archived", uint8_modifier),
    ("os_name", string_modifier),
    ("os_version", string_modifier),
    ("platform", string_modifier),
    ("release", string_modifier),
    ("replay_type", string_modifier),
    ("sdk_name", string_modifier),
    ("sdk_version", string_modifier),
    ("session_sample_rate", f64_modifier),
    ("user_email", string_modifier),
    ("user_id", string_modifier),
    ("user_name", string_modifier),
    ("user", string_modifier),
]
