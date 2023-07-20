from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.groups import (
    DuplicateStorageSetFoundInGroup,
    MigrationGroup,
    build_storage_set_to_group_mapping,
    get_group_loader,
)


def test_load_all_migrations() -> None:
    for group in MigrationGroup:
        group_loader = get_group_loader(group)
        for migration in group_loader.get_migrations():
            group_loader.load_migration(migration)


def test_build_storage_set_to_group_mapping() -> None:
    try:
        storage_set_to_group_mapping = build_storage_set_to_group_mapping()
        assert (
            storage_set_to_group_mapping[StorageSetKey.GENERIC_METRICS_SETS]
            == MigrationGroup.GENERIC_METRICS
        )
        assert (
            storage_set_to_group_mapping[StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS]
            == MigrationGroup.GENERIC_METRICS
        )
        assert (
            storage_set_to_group_mapping[StorageSetKey.EVENTS] == MigrationGroup.EVENTS
        )
        assert (
            storage_set_to_group_mapping[StorageSetKey.TRANSACTIONS]
            == MigrationGroup.TRANSACTIONS
        )
    except DuplicateStorageSetFoundInGroup as e:
        raise e
