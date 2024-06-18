import os
import subprocess


def generate(storage_path: str) -> tuple[str, str]:
    storage_path = os.path.realpath(os.path.abspath(os.path.expanduser(storage_path)))

    # get the version of the file at HEAD
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
        repo_rel_path = os.path.relpath(storage_path, repo_path)
        old_storage = subprocess.run(
            ["git", "show", f"HEAD:{repo_rel_path}"],
            cwd=repo_path,
            capture_output=True,
            check=True,
        ).stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        raise ValueError(e.stderr.decode("utf-8")) from e

    # get the user-provided (modified) storage
    with open(storage_path, "r") as f:
        new_storage = f.read()

    return old_storage, new_storage
