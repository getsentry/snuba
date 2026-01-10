from typing import List, Mapping, Tuple

import pytest

from snuba.migrations import migration
from snuba.migrations.groups import MigrationGroup, get_group_loader

all_migrations: List[Tuple[str, MigrationGroup, migration.ClickhouseNodeMigration]] = []
for group in MigrationGroup:
    group_loader = get_group_loader(group)
    for migration_id in group_loader.get_migrations():
        snuba_migration = group_loader.load_migration(migration_id)
        if isinstance(snuba_migration, migration.ClickhouseNodeMigration):
            all_migrations.append((migration_id, group, snuba_migration))

# For migrations that already use legacy types, set the cutoff to the last index
# new migrations past this point should not use legacy types
legacy_cutoff: Mapping[MigrationGroup, str] = {
    MigrationGroup.SYSTEM: "0001",
    MigrationGroup.DISCOVER: "0007",
    MigrationGroup.EVENTS: "0018",
    MigrationGroup.FUNCTIONS: "0001",
    MigrationGroup.GENERIC_METRICS: "0009",
    MigrationGroup.METRICS: "0034",
    MigrationGroup.OUTCOMES: "0005",
    MigrationGroup.PROFILES: "0004",
    MigrationGroup.QUERYLOG: "0006",
    MigrationGroup.REPLAYS: "0008",
    MigrationGroup.SESSIONS: "0004",
    MigrationGroup.TRANSACTIONS: "0020",
}


def test_legacy_use() -> None:
    """
    check that we don't use legacy types in new migrations that are past the
    cutoff marks
    """
    for migration_id, group, snuba_migration in all_migrations:
        if isinstance(snuba_migration, migration.ClickhouseNodeMigrationLegacy):
            mark = int(migration_id.split("_")[0])
            if (group not in legacy_cutoff) or mark > int(legacy_cutoff[group]):
                pytest.fail(f"Migration {migration_id} in group {group} is using legacy types")
