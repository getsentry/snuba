#!/usr/bin/env python3
import argparse
import subprocess
from shutil import ExecError

"""
This script is meant to be run in CI to check that migrations changes are
not coupled with other changes.
"""


class CoupledMigrations(Exception):
    pass


def _get_migration_changes(workdir: str = ".", to: str = "origin/master") -> str:
    return _get_changes("snuba/snuba_migrations/*/[0-9]*.py", workdir, to)


def _get_changes(glob: str, workdir: str = ".", to: str = "origin/master") -> str:
    migrations_changes = subprocess.run(
        [
            "git",
            "diff",
            "--diff-filter=AM",
            "--name-only",
            to,
            "--",
            glob,
        ],
        stdout=subprocess.PIPE,
        text=True,
        cwd=workdir,
    )
    if migrations_changes.returncode != 0:
        raise ExecError(migrations_changes.stdout)
    else:
        return migrations_changes.stdout


def main(to: str = "origin/master", workdir: str = ".") -> None:
    migrations_changes = _get_migration_changes(workdir, to)
    has_migrations = len(migrations_changes.splitlines()) > 0
    if has_migrations:
        all_changes = _get_changes("*", workdir, to)
        if all_changes != migrations_changes:
            raise CoupledMigrations(
                f"Migration changes are coupled with other changes.\n\nMigrations changes: \n{migrations_changes}\n\nAll changes: \n{all_changes}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", default="origin/master")
    parser.add_argument("--workdir", default=".")
    args = parser.parse_args()
    main(args.to, args.workdir)
