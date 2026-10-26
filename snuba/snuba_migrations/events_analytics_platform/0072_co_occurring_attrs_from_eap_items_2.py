"""Point the co-occurring attributes view at eap_items_2_local.

``eap_item_co_occurring_attrs_3_mv`` (0062) reads ``FROM eap_items_1_local``.
Once the s4s2 consumers write ``eap_items_2_local`` instead, that view stops
firing and ``eap_item_co_occurring_attrs_2_local`` goes stale: no error, no
gap in the table, just attribute autocomplete slowly aging out as TTL expires
the rows it already has.

This adds a second view over the *same* destination table, reading from
``eap_items_2_local``. Both views exist at once and neither is dropped here:

* Creating a view has no effect until something inserts into its source
  table, so this is inert in every environment still writing eap_items_1 --
  the same "dark until flipped" property 0070 relies on.
* In the environment that has cut over, writes land in eap_items_2_local,
  mv_4 fires, and mv_3 goes quiet on its own. The destination table does not
  observe the change.
* On rollback, writes return to eap_items_1_local and mv_3 resumes. Nothing
  needs to be recreated mid-incident.

Only one of the two source tables ever receives writes in a given
environment, so the destination is never double-counted. That matters more
than usual here: the destination is a SummingMergeTree, so a duplicated row
inflates `count` on merge rather than being deduplicated. Dropping mv_3 is
deliberately left to a later migration, once no environment writes
eap_items_1_local.

The projected column set and the SELECT are taken from 0062 unchanged --
only the source table differs. `key_hash` and `attribute_keys_hash` are
MATERIALIZED on the destination table and so are absent from both the column
list and the SELECT; ClickHouse computes them on insert.
"""

from collections.abc import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import (
    Array,
    Column,
    Date,
    DateTime,
    SimpleAggregateFunction,
    String,
    UInt,
)

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

# Must match 0062: the attribute map buckets on eap_items_2_local are copied
# from eap_items_1_local by 0069's CREATE TABLE ... AS, so the count is the
# same on both sides of the cutover.
num_attr_buckets = 40

source_table = "eap_items_2_local"
destination_table = "eap_item_co_occurring_attrs_2_local"

# The family's view numbering is a single counter, not per-source: 0030
# created _1_mv, 0051 replaced it with _2_mv, 0062 with _3_mv. This is _4_mv
# even though it is the first view over eap_items_2_local.
mv_name = "eap_item_co_occurring_attrs_4_mv"

# The destination's column list, minus the two MATERIALIZED columns
# (`key_hash`, `attribute_keys_hash`). A materialized view must not project a
# MATERIALIZED column: the destination computes it from the inserted row.
columns: list[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("item_type", UInt(8)),
    Column("date", Date(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("retention_days", UInt(16)),
    Column("attributes_string", Array(String())),
    Column("attributes_float", Array(String())),
    Column("attributes_int", Array(String())),
    Column("attributes_bool", Array(String())),
    Column("attributes_array_string", Array(String())),
    Column("attributes_array_int", Array(String())),
    Column("attributes_array_float", Array(String())),
    Column("attributes_array_bool", Array(String())),
    Column("count", UInt(64)),
    Column("last_seen", SimpleAggregateFunction("max", [DateTime()])),
]

_attr_num_names = ", ".join([f"mapKeys(attributes_float_{i})" for i in range(num_attr_buckets)])
_attr_str_names = ", ".join([f"mapKeys(attributes_string_{i})" for i in range(num_attr_buckets)])

# Identical to 0062's MV_QUERY apart from the FROM clause. `version` is not
# projected: the destination aggregates attribute-key sets and has no such
# column, and a row's version has no meaning once it is summed into a
# per-(org, project, date, item_type) bucket.
MV_QUERY = f"""
SELECT
    organization_id AS organization_id,
    project_id AS project_id,
    item_type as item_type,
    toMonday(timestamp) AS date,
    retention_days as retention_days,
    arrayConcat({_attr_str_names}) AS attributes_string,
    arrayConcat({_attr_num_names}) AS attributes_float,
    mapKeys(attributes_int) AS attributes_int,
    mapKeys(attributes_bool) AS attributes_bool,
    mapKeys(attributes_array_string) AS attributes_array_string,
    mapKeys(attributes_array_int) AS attributes_array_int,
    mapKeys(attributes_array_float) AS attributes_array_float,
    mapKeys(attributes_array_bool) AS attributes_array_bool,
    1 AS count,
    timestamp AS last_seen
FROM {source_table}
"""


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=storage_set,
                view_name=mv_name,
                columns=columns,
                destination_table_name=destination_table,
                target=OperationTarget.LOCAL,
                query=MV_QUERY,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=storage_set,
                table_name=mv_name,
                target=OperationTarget.LOCAL,
            ),
        ]
