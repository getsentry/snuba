from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Column, UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name="eap_item_co_occurring_attrs_1_local",
                column=Column(
                    "key_hash",
                    UInt(
                        64,
                        # changing the materialized column value to DEFAULT such that materialized views can provide it
                        Modifiers(
                            default="cityHash64(arraySort(arrayConcat(attributes_string, attributes_float, attributes_bool)))"
                        ),
                    ),
                ),
                target=OperationTarget.LOCAL,
            )
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name="eap_item_co_occurring_attrs_1_local",
                column=Column(
                    "key_hash",
                    UInt(
                        64,
                        Modifiers(
                            materialized="cityHash64(arraySort(arrayConcat(attributes_string, attributes_float)))"
                        ),
                    ),
                ),
                target=OperationTarget.LOCAL,
            )
        ]
