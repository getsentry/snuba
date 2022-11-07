from snuba.migrations.groups import MigrationGroup, get_group_loader


def test_load_all_migrations() -> None:
    for group in MigrationGroup:
        group_loader = get_group_loader(group)
        for migration in group_loader.get_migrations():
            group_loader.load_migration(migration)
