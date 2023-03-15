from typing import Iterator, Sequence, Tuple

from snuba.clickhouse.columns import Array, Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (
        Column("dom_tag", String(Modifiers(nullable=True, low_cardinality=True))),
        "tags.value",
    ),
    (
        Column("dom_action", String(Modifiers(nullable=True, low_cardinality=True))),
        "dom_tag",
    ),
    (Column("dom_id", String(Modifiers(nullable=True))), "dom_action"),
    (Column("dom_classes", Array(String())), "dom_id"),
    (Column("dom_aria_label", String(Modifiers(nullable=True))), "dom_classes"),
    (
        Column("dom_aria_role", String(Modifiers(nullable=True, low_cardinality=True))),
        "dom_aria_label",
    ),
    (
        Column("dom_role", String(Modifiers(nullable=True, low_cardinality=True))),
        "dom_aria_role",
    ),
    (Column("dom_text_content", String(Modifiers(nullable=True))), "dom_aria_role"),
    (Column("dom_node_id", UInt(32, Modifiers(nullable=True))), "dom_text_content"),
]

alters: Sequence[str] = ["user", "sdk_name", "sdk_version"]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column, after in columns:
        yield operations.AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=column,
            after=after,
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=column,
            after=after,
            target=operations.OperationTarget.DISTRIBUTED,
        )

    for column_name in alters:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(column_name, String(Modifiers(nullable=True))),
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(column_name, String(Modifiers(nullable=True))),
            target=operations.OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column, _ in columns:
        yield operations.DropColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            target=operations.OperationTarget.DISTRIBUTED,
            column_name=column.name,
        )

        yield operations.DropColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            target=operations.OperationTarget.LOCAL,
            column_name=column.name,
        )

    for column_name in alters:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(column_name, String(Modifiers(nullable=False))),
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(column_name, String(Modifiers(nullable=False))),
            target=operations.OperationTarget.DISTRIBUTED,
        )
