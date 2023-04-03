from typing import Iterator, Sequence, Tuple

from snuba.clickhouse.columns import Array, Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("click_node_id", UInt(32, Modifiers(default="0"))), "tags.value"),
    (
        Column("click_tag", String(Modifiers(default="''", low_cardinality=True))),
        "click_node_id",
    ),
    (Column("click_id", String(Modifiers(default="''"))), "click_tag"),
    (Column("click_class", Array(String())), "click_id"),
    (Column("click_text", String(Modifiers(default="''"))), "click_class"),
    (
        Column("click_role", String(Modifiers(default="''", low_cardinality=True))),
        "click_text",
    ),
    (Column("click_alt", String(Modifiers(default="''"))), "click_role"),
    (Column("click_testid", String(Modifiers(default="''"))), "click_alt"),
    (Column("click_aria_label", String(Modifiers(default="''"))), "click_testid"),
    (Column("click_title", String(Modifiers(default="''"))), "click_aria_label"),
]


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
