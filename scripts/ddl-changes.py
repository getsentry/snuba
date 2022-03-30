import re
import subprocess
from shutil import ExecError
from sys import stderr

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
        lines = diff_result.stdout.splitlines()
        if len(lines) > 0:
            print("-- start migrations")
            print()
        for line in lines:
            # example git diff output:
            # A     snuba/migrations/snuba_migrations/metrics/0030_metrics_distributions_v2_writing_mv.py
            regex = (
                r"(?P<modification_type>[AMD])\t"
                r"snuba/migrations/snuba_migrations/(?P<migration_group>[a-z]+)/(?P<migration_id>[0-9a-z_]+)\.py"
            )

            matches = re.match(regex, line)
            if matches:
                re_groups = matches.groupdict()

                (modification_type, migration_group, migration_id) = (
                    re_groups["modification_type"],
                    MigrationGroup(re_groups["migration_group"]),
                    re_groups["migration_id"],
                )
                # Don't try to dry-run deleted files
                if modification_type in {"M", "A"}:
                    runner = Runner()
                    migration_key = MigrationKey(migration_group, migration_id)
                    print(f"-- migration {migration_group.value} : {migration_id}")
                    runner.run_migration(migration_key, dry_run=True)
                    print(f"-- end migration {migration_group.value} : {migration_id}")
            else:
                # output to stderr doesn't get added to the comment
                print(f"ignoring line from git-diff: {line}", file=stderr)


if __name__ == "__main__":
    _main()
