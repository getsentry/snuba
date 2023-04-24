#!/usr/bin/env python3
import argparse
import subprocess
from shutil import ExecError
from typing import Sequence

"""
This script is meant to be run in CI to check that migrations changes are
not coupled with other changes.
"""

# changes to these files here are allowed to be coupled with migrations
ALLOWED_MIGRATIONS_GLOBS = [
    "snuba/migrations/groups.py",
    "snuba/migrations/group_loader.py",
    "tests",
    "test_distributed_migrations",
    "test_initialization",
    "docs",
    "*.md",
]

# changes to files here are migrations changes
MIGRATIONS_GLOBS = ["snuba/snuba_migrations/*/[0-9]*.py"]


class CoupledMigrations(Exception):
    pass


def _get_migration_changes(workdir: str, to: str) -> str:
    return _get_changes(MIGRATIONS_GLOBS, workdir, to)


def _get_changes(globs: Sequence[str], workdir: str, to: str) -> str:
    changes = subprocess.run(
        [
            "git",
            "diff",
            "--diff-filter=AM",
            "--name-only",
            f"{to}...",
            "--",
            *globs,
            *[f":(exclude){allowed}" for allowed in ALLOWED_MIGRATIONS_GLOBS],
        ],
        stdout=subprocess.PIPE,
        text=True,
        cwd=workdir,
    )
    if changes.returncode != 0:
        raise ExecError(changes.stdout)
    else:
        return changes.stdout


def main(to: str = "origin/master", workdir: str = ".") -> None:
    migrations_changes = _get_migration_changes(workdir, to)
    has_migrations = len(migrations_changes.splitlines()) > 0
    if has_migrations:
        all_changes = _get_changes(["*"], workdir, to)
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
