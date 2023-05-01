from __future__ import annotations

from typing import Union

from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.migrations.groups import get_group_readiness_state_from_storage_set


class StorageValidator:
    def __init__(
        self, storage: Union[ReadableTableStorage, WritableTableStorage]
    ) -> None:
        self.storage = storage

    def validate(self) -> None:
        self.check_readiness_state_level_with_migration_group()

    def check_readiness_state_level_with_migration_group(self) -> None:
        """
        Checks that the storage's readiness state is not greater than the migration group's readiness state.
        For example, there is no case where a storage is `complete` and the associated migration group is in `limited`.
        A storage should not be able to query a non-existing table in ClickHouse.
        This check ensures that the migrations are already deployed before the storage is even loaded.
        """
        storage_set_key = self.storage.get_storage_set_key()
        storage_readiness_state = self.storage.get_readiness_state()
        group_readiness_state = get_group_readiness_state_from_storage_set(
            storage_set_key
        )
        if storage_readiness_state > group_readiness_state:
            raise IncompatibleReadinessStates(
                f"The storage={storage_set_key} readiness state is greater than the corresponding migration group's readiness state."
            )


class IncompatibleReadinessStates(Exception):
    pass
