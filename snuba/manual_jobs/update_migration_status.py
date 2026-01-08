from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec
from snuba.migrations.runner import DIST_TABLE_NAME, LOCAL_TABLE_NAME


class UpdateMigrationStatus(Job):
    """
    UpdateMigrationStatus is a manual job for correcting the status of an
    entry in the migrations_[local|dist] table, to be run via the snuba jobs CLI
    or via snuba admin (this should exclusively be instantiated from the manual
    jobs runner, not imported by other packages).

    It requires the following parameters:
        - group - migration group that the entry belongs to
        - migration_id - specific ID for the entry
        - old_status - the current entry's status in the table (to make sure the user understands what they're
          updating from)
        - new_status - the status that the entry should contain after the update is run.

    It uses an INSERT rather than an ALTER TABLE ... UPDATE in order to keep record-keeping
    in the migrations table sensible.
    """

    def __init__(self, job_spec: JobSpec):
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params is not None, "group, migration_id, old_status, new_status parameters required"
        assert params["group"], "group required"
        assert params["migration_id"], "migration_id required"
        assert params["new_status"], "new_status required"
        assert params["old_status"], "old_status required"
        self._group = params["group"]
        self._migration_id = params["migration_id"]
        self._new_status = params["new_status"]
        self._old_status = params["old_status"]
        assert isinstance(self._group, str)
        assert isinstance(self._migration_id, str)
        assert isinstance(self._new_status, str)
        assert isinstance(self._old_status, str)

    def _select_query(self, table_name: str) -> str:
        return f"SELECT status, version FROM {table_name} FINAL WHERE group = %(group)s AND migration_id = %(migration_id)s;"

    def _insert_query(self, table_name: str) -> str:
        return f"INSERT INTO {table_name} FORMAT JSONEachRow"

    def execute(self, logger: JobLogger) -> None:
        migrations_cluster = get_cluster(StorageSetKey.MIGRATIONS)
        table_name = LOCAL_TABLE_NAME if migrations_cluster.is_single_node() else DIST_TABLE_NAME
        connection = migrations_cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

        existing_row = connection.execute(
            self._select_query(table_name),
            {"migration_id": self._migration_id, "group": self._group},
        )
        assert (
            existing_row and len(existing_row.results) > 0
        ), f"cannot find row with group {self._group}, ID {self._migration_id} in table {table_name}: no update to be performed"
        version = existing_row.results[0][1]
        status = existing_row.results[0][0]

        assert (
            status == self._old_status
        ), f"actual status {status} does not match expected {self._old_status}, aborting"

        query = self._insert_query(table_name)
        row_data = [
            {
                "group": self._group,
                "migration_id": self._migration_id,
                "timestamp": datetime.now(),
                "status": self._new_status,
                "version": version + 1,
            }
        ]

        logger.info(f"{self.job_spec.job_id}: executing {query} with data = {row_data}")

        result = connection.execute(query, row_data)
        logger.info(result.__repr__())
