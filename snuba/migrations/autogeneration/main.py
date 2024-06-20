import os
import subprocess

from snuba.migrations.autogeneration.diff import generate_migration


def generate(storage_path: str) -> None:
    # load into memory the given storage and the version of it at HEAD
    new_storage, old_storage = get_working_and_head(storage_path)

    # generate the migration operations
    generate_migration(old_storage, new_storage)


def get_working_and_head(path: str) -> tuple[str, str]:
    """
    Given a path to a file, returns the contents of the file in the working directory
    and the contents of it at HEAD in the git repo, as a tuple: (working, head)

    preconditions:
        - path is a valid path to a file in a git repo
    """
    path = os.path.realpath(os.path.abspath(os.path.expanduser(path)))
    # get the version at HEAD
    try:
        repo_path = (
            subprocess.run(
                [
                    "git",
                    "rev-parse",
                    "--show-toplevel",
                ],
                cwd=os.path.dirname(path),
                capture_output=True,
                check=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )
        repo_rel_path = os.path.relpath(path, repo_path)
        head_file = subprocess.run(
            ["git", "show", f"HEAD:{repo_rel_path}"],
            cwd=repo_path,
            capture_output=True,
            check=True,
        ).stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        raise ValueError(e.stderr.decode("utf-8")) from e

    # working
    with open(path, "r") as f:
        working_file = f.read()

    return (working_file, head_file)
