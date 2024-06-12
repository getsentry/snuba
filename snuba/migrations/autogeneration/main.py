import os
import subprocess


def generate(storage_path: str) -> None:
    repo_path = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True
    ).stdout.decode("utf-8")
    storage_path = os.path.abspath(os.path.expanduser(storage_path))
    if not storage_path.startswith(repo_path):
        raise ValueError("Storage path is not in the git repository")

    rel_path = os.path.relpath(storage_path, repo_path)
    old_storage = ""
    new_storage = ""
