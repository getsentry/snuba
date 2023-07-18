#!/usr/bin/env python3
import argparse
import os
import subprocess
from shutil import ExecError
from typing import Optional, Sequence

import requests

"""
This script is meant to be run in CI to check that migrations changes are
not coupled with other changes.
"""

# changes to these files here are allowed to be coupled with migrations
ALLOWED_MIGRATIONS_GLOBS = [
    "snuba/migrations/groups.py",
    "snuba/migrations/group_loader.py",
    "snuba/settings/*",
    "snuba/clusters/storage_sets.py",
    "tests",
    "test_distributed_migrations",
    "test_initialization",
    "docs",
    "*.md",
]

# changes to files here are migrations changes
MIGRATIONS_GLOBS = ["snuba/snuba_migrations/*/[0-9]*.py"]

SKIP_LABEL = "skip-check-migrations"
BASE_REF = "master"


class CoupledMigrations(Exception):
    pass


def _get_head_sha(workdir: str) -> str:
    return (
        subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workdir)
        .decode()
        .strip()
    )


def _pr_has_skip_label(commit_sha: str) -> bool:
    GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
    if not GITHUB_TOKEN:
        print("GITHUB_TOKEN not set. Skipping github PR labels check.")
        return False

    # get the PR from the commit using the github api
    # and check its labels
    # see: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-pull-requests-associated-with-a-commit
    pull_request_url = (
        "https://api.github.com/repos/getsentry/snuba/commits/" + commit_sha + "/pulls"
    )

    # Make the request
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {GITHUB_TOKEN}",
        # A user agent is required.
        # https://docs.github.com/en/rest/overview/resources-in-the-rest-api#user-agent-required
        "User-Agent": "getsentry/gocd-agent",
    }
    response = requests.get(pull_request_url, headers=headers)

    # check associated merged PR labels
    if response.status_code == 200:
        pulls = response.json()
        for pull in pulls:
            if pull["merged_at"] is None or pull["base"]["ref"] != BASE_REF:
                continue
            labels = pull["labels"]
            for label in labels:
                if SKIP_LABEL in label["name"]:
                    return True
    else:
        print("Error checking github:", response.status_code)

    return False


def _head_commit_pr_has_skip_label(workdir: str) -> bool:
    head_sha = _get_head_sha(workdir)
    print("Checking if PR for commit", head_sha, "has skip label")
    return _pr_has_skip_label(head_sha)


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
    to: str = "origin/master", workdir: str = ".", labels: Optional[Sequence[str]] = []
) -> None:
    if labels:
        for label in labels:
            if SKIP_LABEL in label:
                return

    if _head_commit_pr_has_skip_label(workdir):
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
    parser.add_argument("--labels", nargs="*")
    args = parser.parse_args()
    print(
        f"migrations changes: to: {args.to}, workdir: {args.workdir}, labels: {args.labels}"
    )
    main(args.to, args.workdir, args.labels)
