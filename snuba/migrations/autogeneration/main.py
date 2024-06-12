import os
import subprocess

import requests
import yaml


def generate(local_storage_path: str) -> tuple[str, str]:
    local_repo_path = (
        subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True)
        .stdout.decode("utf-8")
        .strip()
    )
    local_storage_path = os.path.abspath(os.path.expanduser(local_storage_path))
    if not local_storage_path.startswith(local_repo_path):
        raise ValueError(
            f"Storage path '{local_storage_path}' is not in the git repository '{local_repo_path}'"
        )

    # get local storage
    rel_storage_path = os.path.relpath(local_storage_path, local_repo_path)
    with open(os.path.join(local_repo_path, rel_storage_path), "r") as f:
        new_storage = yaml.safe_load(f)

    # get remote storage
    REMOTE_SNUBA_REPO = "https://raw.githubusercontent.com/getsentry/snuba/master"
    res = requests.get(f"{REMOTE_SNUBA_REPO}/{rel_storage_path}")
    old_storage = yaml.safe_load(res.text)

    return old_storage, new_storage
