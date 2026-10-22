"""Add ZSTD(1) to the attributes_bool and attributes_int map columns.

These are the only two attribute map families without an explicit codec: 0024
gave attributes_string_* and attributes_float_* ZSTD(1) (copied from
eap_spans), but bool and int were new column kinds with no precedent and fell
back to the LZ4 default.
"""

from snuba.clickhouse.columns import Bool, Column, Int, Map, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

local_tables = [
    "eap_items_1_local",
    "eap_items_1_downsample_8_local",
    "eap_items_1_downsample_64_local",
    "eap_items_1_downsample_512_local",
    "eap_items_2_local",
    "eap_items_2_downsample_8_local",
    "eap_items_2_downsample_64_local",
    "eap_items_2_downsample_512_local",
]


def _columns(codecs: list[str] | None) -> list[Column[Modifiers]]:
    modifiers = Modifiers(codecs=codecs) if codecs else None
    return [
        Column("attributes_bool", Map(String(), Bool(), modifiers=modifiers)),
        Column("attributes_int", Map(String(), Int(64), modifiers=modifiers)),
    ]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=storage_set,
                table_name=table,
                column=column,
                target=OperationTarget.LOCAL,
            )
            for table in local_tables
            for column in _columns(["ZSTD(1)"])
        ]

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=storage_set,
                table_name=table,
                column=column,
                target=OperationTarget.LOCAL,
            )
            for table in local_tables
            # REMOVE CODEC not supported, so rollback declares LZ4 explicitly
            for column in _columns(["LZ4"])
        ]
