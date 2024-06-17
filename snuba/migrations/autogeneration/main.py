import os
import subprocess

import yaml


def generate(storage_path: str) -> tuple[str, str]:
    storage_path = os.path.abspath(os.path.expanduser(storage_path))

    # get the version of the storage at HEAD
    try:
        repo_path = (
            subprocess.run(
                [
                    "git",
                    "rev-parse",
                    "--show-toplevel",
                ],
                cwd=os.path.dirname(storage_path),
                capture_output=True,
                check=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )
        if not repo_path.endswith("snuba"):
            raise ValueError(
                "expected git repo to end with 'snuba' but got: " + repo_path
            )
        assert storage_path.startswith(repo_path)  # should always hold
        rel_storage_path = storage_path[len(repo_path) + 1 :]
        old_storage = (
            subprocess.run(
                ["git", "show", f"HEAD:{rel_storage_path}"],
                capture_output=True,
                check=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )
    except subprocess.CalledProcessError as e:
        raise ValueError(e.stderr.decode("utf-8")) from e
    old_storage = yaml.safe_load(old_storage)

    # get the user-provided (modified) storage
    with open(storage_path, "r") as f:
        new_storage = yaml.safe_load(f)

    return old_storage, new_storage
