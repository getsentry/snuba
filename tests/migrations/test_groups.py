from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.groups import MigrationGroup, get_group_loader


def test_load_all_migration_groups_have_readiness_state() -> None:
    for group in MigrationGroup:
        assert hasattr(group, "_readiness_state_")
        assert isinstance(group.readiness_state, ReadinessState)


def test_load_all_migrations() -> None:
    for group in MigrationGroup:
        group_loader = get_group_loader(group)
        for migration in group_loader.get_migrations():
            group_loader.load_migration(migration)
