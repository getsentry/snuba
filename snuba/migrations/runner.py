import logging

from datetime import datetime
from functools import partial

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster, CLUSTERS
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.context import Context
from snuba.migrations.groups import get_group_loader, MigrationGroup
from snuba.migrations.status import Status

logger = logging.getLogger("snuba.migrations")

LOCAL_TABLE_NAME = "migrations_local"
DIST_TABLE_NAME = "migrations_dist"

# Currently only local migrations are supported
TABLE_NAME = LOCAL_TABLE_NAME


class Runner:
    def __init__(self) -> None:
        self.__connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

    def run_migration(self, group: MigrationGroup, migration_id: str) -> None:
        """
        Run a single migration given its migration key and marks the migration as complete.
        """
        assert all(
            cluster.is_single_node() for cluster in CLUSTERS
        ), "Cannot run migrations for multi node clusters"

        context = Context(
            migration_id,
            logger,
            partial(self._update_migration_status, group, migration_id),
        )
        migration = get_group_loader(group).load_migration(migration_id)
        migration.forwards(context)

    def _update_migration_status(
        self, group: MigrationGroup, migration_id: str, status: Status
    ) -> None:
        statement = f"INSERT INTO {TABLE_NAME} FORMAT JSONEachRow"
        data = [
            {
                "group": group.value,
                "migration_id": migration_id,
                "timestamp": datetime.now(),
                "status": status.value,
            }
        ]

        self.__connection.execute(statement, data)
