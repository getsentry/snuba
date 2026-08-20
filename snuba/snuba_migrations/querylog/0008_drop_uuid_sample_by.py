from collections.abc import Sequence

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

TABLE_NAME = "querylog_local"

# Pre-ClickHouse 21.9 allowed SAMPLE BY on UUID columns. querylog_local used
# SAMPLE BY request_id (a UUID). ClickHouse now rejects that type, so SHOW CREATE
# / cluster expansion fails with ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER.
# See https://github.com/getsentry/snuba/issues/7216
ILLEGAL_SAMPLE_BY = "request_id"


class DropUuidSampleBy(operations.SqlOperation):
    """Inspects ClickHouse at execute time, then removes SAMPLE BY if it is illegal."""

    def __init__(self) -> None:
        super().__init__(StorageSetKey.QUERYLOG, target=operations.OperationTarget.LOCAL)

    def format_sql(self) -> str:
        on_cluster = self._get_on_cluster_clause()
        return f"ALTER TABLE {TABLE_NAME}{on_cluster} REMOVE SAMPLE BY;"

    def execute(self) -> None:
        cluster = get_cluster(StorageSetKey.QUERYLOG)
        nodes = cluster.get_local_nodes()
        if not nodes:
            return

        clickhouse = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, nodes[0])
        sampling_key_rows = clickhouse.execute(
            "SELECT sampling_key FROM system.tables "
            f"WHERE name = '{TABLE_NAME}' AND database = '{cluster.get_database()}'"
        ).results
        if not sampling_key_rows or sampling_key_rows[0][0] != ILLEGAL_SAMPLE_BY:
            return

        super().execute()


class Migration(migration.ClickhouseNodeMigration):
    """
    Drop the illegal UUID SAMPLE BY request_id from querylog_local in place.

    Fresh installs after 0001 no longer create this clause (#4842). Existing
    clusters that still have it cannot dump or recreate the table on
    ClickHouse >= 21.9 (https://github.com/getsentry/snuba/issues/7216).
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [DropUuidSampleBy()]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # Do not restore the illegal SAMPLE BY clause.
        return []
