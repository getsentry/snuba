from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Array, Column, UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name="eap_item_co_occurring_attrs_1_local",
                column=Column(
                    "attribute_keys_hash",
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=get_array_vals_hash(
                                "arrayDistinct(arrayConcat(attributes_string, attributes_float, attributes_bool))"
                            )
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
                    "attribute_keys_hash",
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=get_array_vals_hash(
                                "arrayConcat(attributes_string, attributes_float)"
                            )
                        ),
                    ),
                ),
                target=OperationTarget.LOCAL,
            )
        ]
