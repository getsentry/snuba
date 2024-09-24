from datetime import datetime
from typing import Mapping

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobSpec, logger
from snuba.migrations.runner import DIST_TABLE_NAME, LOCAL_TABLE_NAME


class UpdateMigrationStatus(Job):
    def __init__(
        self,
        job_spec: JobSpec,
        dry_run: bool,
    ):
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec, dry_run)

    def __validate_job_params(self, params: Mapping[str, str]) -> None:
        assert params["group"], "group required"
        assert params["migration_id"], "migration_id required"
        assert params["new_status"], "new_status required"
        assert params["old_status"], "old_status required"
        self._group = params["group"]
        self._migration_id = params["migration_id"]
        self._new_status = params["new_status"]
        self._old_status = params["old_status"]

    def _select_query(self, table_name: str) -> str:
        return f"SELECT status, version FROM {table_name} FINAL WHERE group = %(group)s AND migration_id = %(migration_id)s;"

    # by INSERTing instead of ALTER TABLE, we update the version to make it clear
    # that a change occurred
    def _insert_query(self, table_name: str) -> str:
        return f"INSERT INTO {table_name} FORMAT JSONEachRow"

    def execute(self) -> None:
        migrations_cluster = get_cluster(StorageSetKey.MIGRATIONS)
        table_name = (
            LOCAL_TABLE_NAME if migrations_cluster.is_single_node() else DIST_TABLE_NAME
        )
        connection = migrations_cluster.get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

        existing_row = connection.execute(
            self._select_query(table_name),
            {"migration_id": self._migration_id, "group": self._group},
        )
        assert (
            existing_row and len(existing_row.results) > 0
        ), f"cannot find row with group {self._group}, ID {self._migration_id} in table {table_name}: no update to be performed"
        version = existing_row.results[0][1]
        status = existing_row.results[0][0]

        logger.debug(existing_row)
        logger.debug(version)
        logger.debug(status)

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
        logger.info(result)
