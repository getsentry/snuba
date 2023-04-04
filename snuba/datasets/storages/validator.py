from typing import Union

from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.migrations.groups import get_group_readiness_state_from_storage_set


class StorageValidator:
    def __init__(
        self, storage: Union[ReadableTableStorage, WritableTableStorage]
    ) -> None:
        self.storage = storage

    def validate(self) -> None:
        pass

    def check_readiness_state_level_with_migration_group(self) -> None:
        storage_set_key = self.storage.get_storage_set_key()
        storage_readiness_state = self.storage.get_readiness_state()
        group_readiness_state = get_group_readiness_state_from_storage_set(
            storage_set_key
        )

        if storage_readiness_state.level > group_readiness_state.level:
            raise Exception()
