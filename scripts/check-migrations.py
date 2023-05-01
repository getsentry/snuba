#!/usr/bin/env python3
import argparse
import subprocess
from shutil import ExecError
from typing import Optional, Sequence

"""
This script is meant to be run in CI to check that migrations changes are
not coupled with other changes.
"""

# changes to these files here are allowed to be coupled with migrations
ALLOWED_MIGRATIONS_GLOBS = [
    "snuba/migrations/groups.py",
    "snuba/migrations/group_loader.py",
    "snuba/settings/*",
    "tests",
    "test_distributed_migrations",
    "test_initialization",
    "docs",
    "*.md",
]

# changes to files here are migrations changes
MIGRATIONS_GLOBS = ["snuba/snuba_migrations/*/[0-9]*.py"]

SKIP_LABEL = "skip-migrations-check"


class CoupledMigrations(Exception):
    pass


def _has_skip_label(label: Optional[str]) -> bool:
    # check the notes from the commit
    notes = subprocess.run(
        ["git", "notes", "show"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if notes.returncode != 0:
        if "no note found" not in notes.stderr:
            raise ExecError(notes.stdout)
    if SKIP_LABEL in notes.stdout:
        return True

    if label == SKIP_LABEL:
        # add a note to the commit, so GOCD can see it
        add_notes_change = subprocess.run(
            ["git", "notes", "append", "-m", f"skipped migrations check: {SKIP_LABEL}"]
        )
        if add_notes_change.returncode != 0:
            raise ExecError(add_notes_change.stdout)
        push_notes_change = subprocess.run(["git", "push", "origin", "refs/notes/*"])
        if push_notes_change.returncode != 0:
            raise ExecError(push_notes_change.stdout)
        return True
    return False


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


def main(
    to: str = "origin/master", workdir: str = ".", label: Optional[str] = None
) -> None:
    if _has_skip_label(label):
        return
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
    parser.add_argument("--label")
    args = parser.parse_args()
    main(args.to, args.workdir, args.label)
