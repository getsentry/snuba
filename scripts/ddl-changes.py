import os.path
import subprocess
from shutil import ExecError

from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationKey, Runner


def _main() -> None:
    """
    This method takes the output of `git diff --name-status master snuba/migrations` and
    runs `snuba migrations run -dry-run with the proper parameters`, for a CI action
    """
    diff_result = subprocess.run(
        [
            "git",
            "diff",
            "--diff-filter=AM",
            "--name-only",
            "origin/master",
            "--",
            "snuba/snuba_migrations/*/[0-9]*.py",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    if diff_result.returncode != 0:
        raise ExecError(diff_result.stdout)
    else:
        lines = diff_result.stdout.splitlines()
        if len(lines) > 0:
            print("-- start migrations")
            print()
        for line in lines:
            migration_filename = os.path.basename(line)
            migration_group = MigrationGroup(os.path.basename(os.path.dirname(line)))
            migration_id, _ = os.path.splitext(migration_filename)
            runner = Runner()
            migration_key = MigrationKey(migration_group, migration_id)
            print(f"-- forward migration {migration_group.value} : {migration_id}")
            runner.run_migration(migration_key, dry_run=True)
            print(f"-- end forward migration {migration_group.value} : {migration_id}")

            print("\n\n\n")
            migration_key = MigrationKey(migration_group, migration_id)
            print(f"-- backward migration {migration_group.value} : {migration_id}")
            runner.reverse_migration(migration_key, dry_run=True)
            print(f"-- end backward migration {migration_group.value} : {migration_id}")


if __name__ == "__main__":
    _main()
