from typing import Sequence, TypedDict

from snuba.migrations.groups import MigrationGroup


class MigrationGroupData(TypedDict):
    group: MigrationGroup
    migration_ids: Sequence[str]
