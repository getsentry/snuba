from typing import Sequence

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Nested


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    storage_set_key = StorageSetKey.EVENTS

    local_table_name = "errors_local"
    dist_table_name = "errors_dist"

    column_before = Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String(Modifiers(nullable=True))),
                ("colno", UInt(32, Modifiers(nullable=True))),
                ("filename", String(Modifiers(nullable=True))),
                ("function", String(Modifiers(nullable=True))),
                ("lineno", UInt(32, Modifiers(nullable=True))),
                ("in_app", UInt(8, Modifiers(nullable=True))),
                ("package", String(Modifiers(nullable=True))),
                ("module", String(Modifiers(nullable=True))),
                ("stack_level", UInt(16, Modifiers(nullable=True))),
            ]
        ),
    )

    column_after = Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String(Modifiers(nullable=True))),
                ("colno", UInt(32, Modifiers(nullable=True))),
                ("filename", String(Modifiers(nullable=True))),
                ("function", String(Modifiers(nullable=True))),
                ("lineno", UInt(32, Modifiers(nullable=True))),
                ("in_app", UInt(8, Modifiers(nullable=True))),
                ("package", String(Modifiers(nullable=True))),
                ("module", String(Modifiers(nullable=True))),
                ("stack_level", UInt(16, Modifiers(nullable=True))),
                ("sourcemapped", UInt(8, Modifiers(nullable=True))),
            ]
        ),
    )

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column=self.column_after,
                target=target,
            )
            for table_name, target in [
                (self.local_table_name, OperationTarget.LOCAL),
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column=self.column_before,
                target=target,
            )
            for table_name, target in [
                (self.local_table_name, OperationTarget.LOCAL),
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]
