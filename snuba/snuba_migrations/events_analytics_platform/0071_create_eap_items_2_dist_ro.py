"""Create the read-only distributed tables for eap_items_2.

Step 3 of moving eap_items onto a ReplacingMergeTree keyed on `version`:
0069 created the eap_items_2 tables, 0070 wired up their downsample
materialized views, and this adds the `_dist_ro` counterparts so the
read-only routing path has somewhere to point once reads cut over.

`enable_eap_readonly_table` routes non-consistent EAP reads to the
`events_analytics_platform_ro` storage set, so without these tables a v2
read cutover would break for every deployment where that option is on.

Mirrors 0056_eap_items_dist_ro, including its choice of template: the new
tables are created `AS <the v2 distributed table>`, not `AS <the v2 local
table>`. Distributed tables are created ON CLUSTER over the query nodes,
which do not host the local tables -- those live on the storage cluster --
so templating off a local table fails there with "Table ... does not
exist". 0069 hit exactly that.

These tables carry no data of their own; they are Distributed views onto
the eap_items_2 local tables.
"""

from collections.abc import Sequence

from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO

# Matches 0024_items / 0056_eap_items_dist_ro / 0069. A different sharding
# key here would scatter a trace differently than the tables these read.
SHARDING_KEY = "cityHash64(reinterpretAsUInt128(trace_id))"

# (new dist_ro table, template dist table, local table it reads)
DIST_RO_TABLES: list[tuple[str, str, str]] = [
    (
        "eap_items_2_dist_ro",
        "eap_items_2_dist",
        "eap_items_2_local",
    ),
    (
        "eap_items_2_downsample_8_dist_ro",
        "eap_items_2_downsample_8_dist",
        "eap_items_2_downsample_8_local",
    ),
    (
        "eap_items_2_downsample_64_dist_ro",
        "eap_items_2_downsample_64_dist",
        "eap_items_2_downsample_64_local",
    ),
    (
        "eap_items_2_downsample_512_dist_ro",
        "eap_items_2_downsample_512_dist",
        "eap_items_2_downsample_512_local",
    ),
]


def _on_cluster_clause() -> str:
    cluster = get_cluster(storage_set)
    if cluster.is_single_node():
        return ""
    name = cluster.get_clickhouse_distributed_cluster_name()
    return f" ON CLUSTER `{name}`" if name else ""


def _create_dist_ro_sql(new_table: str, template_table: str, local_table: str) -> str:
    cluster = get_cluster(storage_set)
    cluster_name = cluster.get_clickhouse_cluster_name()
    database = cluster.get_database()
    return (
        f"CREATE TABLE IF NOT EXISTS {new_table}{_on_cluster_clause()} "
        f"AS {template_table} "
        f"ENGINE = Distributed(`{cluster_name}`, {database}, {local_table}, {SHARDING_KEY})"
    )


def _drop_dist_ro_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS {table}{_on_cluster_clause()}"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.RunSql(
                storage_set=storage_set,
                statement=_create_dist_ro_sql(new_table, template_table, local_table),
                target=OperationTarget.DISTRIBUTED,
            )
            for new_table, template_table, local_table in DIST_RO_TABLES
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.RunSql(
                storage_set=storage_set,
                statement=_drop_dist_ro_sql(new_table),
                target=OperationTarget.DISTRIBUTED,
            )
            for new_table, *_ in DIST_RO_TABLES
        ]
