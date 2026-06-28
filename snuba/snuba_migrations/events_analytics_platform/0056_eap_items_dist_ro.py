from collections.abc import Sequence

from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO

# (new_dist_ro_table, source_dist_table, local_table, sharding_key)
DIST_RO_TABLES: list[tuple[str, str, str, str | None]] = [
    (
        "eap_items_1_dist_ro",
        "eap_items_1_dist",
        "eap_items_1_local",
        "cityHash64(reinterpretAsUInt128(trace_id))",
    ),
    (
        "eap_items_1_downsample_8_dist_ro",
        "eap_items_1_downsample_8_dist",
        "eap_items_1_downsample_8_local",
        "cityHash64(reinterpretAsUInt128(trace_id))",
    ),
    (
        "eap_items_1_downsample_64_dist_ro",
        "eap_items_1_downsample_64_dist",
        "eap_items_1_downsample_64_local",
        "cityHash64(reinterpretAsUInt128(trace_id))",
    ),
    (
        "eap_items_1_downsample_512_dist_ro",
        "eap_items_1_downsample_512_dist",
        "eap_items_1_downsample_512_local",
        "cityHash64(reinterpretAsUInt128(trace_id))",
    ),
    (
        "eap_item_co_occurring_attrs_1_dist_ro",
        "eap_item_co_occurring_attrs_1_dist",
        "eap_item_co_occurring_attrs_1_local",
        None,
    ),
]


def _on_cluster_clause() -> str:
    cluster = get_cluster(storage_set)
    if cluster.is_single_node():
        return ""
    name = cluster.get_clickhouse_distributed_cluster_name()
    return f" ON CLUSTER `{name}`" if name else ""


def _create_dist_ro_sql(
    new_table: str,
    source_table: str,
    local_table: str,
    sharding_key: str | None,
) -> str:
    cluster = get_cluster(storage_set)
    cluster_name = cluster.get_clickhouse_cluster_name()
    database = cluster.get_database()
    sharding = f", {sharding_key}" if sharding_key else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {new_table}{_on_cluster_clause()} "
        f"AS {source_table} "
        f"ENGINE = Distributed(`{cluster_name}`, {database}, {local_table}{sharding})"
    )


def _drop_dist_ro_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS {table}{_on_cluster_clause()}"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.RunSql(
                storage_set=storage_set,
                statement=_create_dist_ro_sql(new, src, local, key),
                target=OperationTarget.DISTRIBUTED,
            )
            for new, src, local, key in DIST_RO_TABLES
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.RunSql(
                storage_set=storage_set,
                statement=_drop_dist_ro_sql(new),
                target=OperationTarget.DISTRIBUTED,
            )
            for new, *_ in DIST_RO_TABLES
        ]
