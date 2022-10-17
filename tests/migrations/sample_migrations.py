from typing import List, Sequence, Tuple

from snuba.migrations.groups import DirectoryLoader, GroupLoader, MigrationGroup


class ExampleLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("tests.migrations.examples")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_sql_migration", "0002_code_migration"]


REGISTERED_GROUPS_EX: List[Tuple[str, GroupLoader]] = [
    ("examples", ExampleLoader()),
]

REGISTERED_GROUPS_LOOKUP_EX = {k: v for (k, v) in REGISTERED_GROUPS_EX}


def get_group_loader_example(group: MigrationGroup) -> GroupLoader:
    return REGISTERED_GROUPS_LOOKUP_EX[group]
