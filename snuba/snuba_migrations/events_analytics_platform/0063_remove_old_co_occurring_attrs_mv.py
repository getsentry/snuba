from collections.abc import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import get_array_vals_hash
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Array, Column, Date, String, UInt

num_attr_buckets = 40

columns: list[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("item_type", UInt(8)),
    Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column(
        "retention_days",
        UInt(16),
    ),
    Column(
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
    Column("attributes_string", Array(String())),
    Column("attributes_float", Array(String())),
    Column("attributes_bool", Array(String())),
    # a hash of all the attribute keys of the item in sorted order
    # this lets us deduplicate rows with merges
    Column(
        "key_hash",
        UInt(
            64,
            Modifiers(
                materialized="cityHash64(arraySort(arrayDistinct(arrayConcat(attributes_string, attributes_float, attributes_bool))))"
            ),
        ),
    ),
]

_attr_num_names = ", ".join([f"mapKeys(attributes_float_{i})" for i in range(num_attr_buckets)])
_attr_str_names = ", ".join([f"mapKeys(attributes_string_{i})" for i in range(num_attr_buckets)])

MV_QUERY = f"""
SELECT
    organization_id AS organization_id,
    project_id AS project_id,
    item_type as item_type,
    toMonday(timestamp) AS date,
    retention_days as retention_days,
    arrayConcat({_attr_str_names}) AS attributes_string,
    mapKeys(attributes_bool) AS attributes_bool,
    arrayConcat({_attr_num_names}) AS attributes_float
FROM eap_items_1_local
"""


class Migration(migration.ClickhouseNodeMigration):
    """
    Stop writing to the v1 co-occurring attributes table.

    Reads have moved (or are moving) to ``eap_item_co_occurring_attrs_2_*``
    via ``eap_item_co_occurring_attrs_3_mv``. Dropping this view ends the
    dual-write; the v1 local/dist tables are left in place for a later drop.
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    local_table_name = "eap_item_co_occurring_attrs_1_local"
    mv_name = "eap_item_co_occurring_attrs_2_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.mv_name,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.mv_name,
                columns=columns,
                destination_table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
                query=MV_QUERY,
            ),
        ]
