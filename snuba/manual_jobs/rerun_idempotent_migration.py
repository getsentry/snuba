from typing import Any, Mapping, Optional

from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec
from snuba.migrations.groups import get_group_loader
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


class RerunIdempotentMigration(Job):
    """
    Reruns the forward SQL operations of a completed idempotent migration.

    Parameters:
        - storage_set: The StorageSetKey value (e.g. "events_analytics_platform")
        - migration_id: The migration ID (e.g. "0001_events_initial")
    """

    def __init__(self, job_spec: JobSpec) -> None:
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Optional[Mapping[Any, Any]]) -> None:
        assert params is not None, "storage_set and migration_id parameters required"
        assert params.get("storage_set"), "storage_set required"
        assert params.get("migration_id"), "migration_id required"
        self._storage_set = params["storage_set"]
        self._migration_id = params["migration_id"]
        assert isinstance(self._storage_set, str)
        assert isinstance(self._migration_id, str)

    def execute(self, logger: JobLogger) -> None:
        storage_set_key = StorageSetKey(self._storage_set)

        # Find the migration group for this storage set
        from snuba.migrations.groups import _STORAGE_SET_TO_MIGRATION_GROUP_MAPPING

        group = _STORAGE_SET_TO_MIGRATION_GROUP_MAPPING.get(storage_set_key)
        assert group is not None, f"No migration group found for storage set {self._storage_set}"

        # Check that the migration is in COMPLETED state
        migration_key = MigrationKey(group=group, migration_id=self._migration_id)
        runner = Runner()
        status, _ = runner.get_status(migration_key)
        assert status == Status.COMPLETED, (
            f"Migration {self._migration_id} in group {group.value} has status "
            f"{status.value}, expected completed"
        )

        # Load the migration
        group_loader = get_group_loader(group)
        migration = group_loader.load_migration(self._migration_id)
        assert isinstance(migration, ClickhouseNodeMigration), (
            f"Migration {self._migration_id} is not a ClickhouseNodeMigration, "
            f"only SQL migrations are supported"
        )

        # Get and execute forward operations
        ops = migration.forwards_ops()
        logger.info(
            f"{self.job_spec.job_id}: rerunning {len(ops)} operations from "
            f"migration {self._migration_id} (group={group.value})"
        )

        for i, op in enumerate(ops):
            sql = op.format_sql()
            logger.info(f"{self.job_spec.job_id}: executing op {i + 1}/{len(ops)}: {sql}")
            op.execute()
            logger.info(f"{self.job_spec.job_id}: op {i + 1}/{len(ops)} completed")

        logger.info(
            f"{self.job_spec.job_id}: successfully reran all operations for "
            f"migration {self._migration_id}"
        )
