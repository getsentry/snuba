from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
table_name_prefix = "eap_items_1"

new_column_name = "version"

# Milliseconds since the unix epoch at the time the row is written.
default_expression = "toUnixTimestamp64Milli(now64(3))"

new_column: Column[Modifiers] = Column(
    new_column_name,
    UInt(
        64,
        modifiers=Modifiers(
            default=default_expression,
            codecs=["ZSTD(1)"],
        ),
    ),
)


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_local",
                column=new_column,
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_dist",
                column=new_column,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_dist",
                column_name=new_column_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_local",
                column_name=new_column_name,
                target=OperationTarget.LOCAL,
            ),
        ]
