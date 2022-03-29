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
        ["git", "diff", "--name-status", "origin/master", "--", "snuba/migrations"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if diff_result.returncode != 0:
        raise ExecError(diff_result.stdout)
    else:
        for line in diff_result.stdout.splitlines():
            (modification_type, filename) = line.split()
            runner = Runner()
            migration_group = MigrationGroup("metrics")
            migration_key = MigrationKey(
                migration_group, "0020_polymorphic_buckets_table"
            )

            runner.run_migration(migration_key, dry_run=True)
            print(modification_type)
            print(filename)
            exit(0)


if __name__ == "__main__":
    _main()
