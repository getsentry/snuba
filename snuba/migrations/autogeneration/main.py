import os
import subprocess
from typing import Optional

from yaml import safe_load

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import group_loader
from snuba.migrations.autogeneration.diff import generate_python_migration


def generate(storage_path: str, migration_name: Optional[str] = None) -> str:
    # load into memory the given storage and the version of it at HEAD
    tmpnew, tmpold = get_working_and_head(storage_path)
    new_storage = safe_load(tmpnew)
    old_storage = safe_load(tmpold)

    # generate the migration operations
    migration = generate_python_migration(old_storage, new_storage)

    # write the migration and return the path
    return write_migration(
        migration,
        StorageSetKey(new_storage["storage"]["set_key"]),
        migration_name if migration_name else "generated_migration",
    )


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


def write_migration(
    migration: str,
    storage_set: StorageSetKey,
    migration_name: str,
) -> str:
    """
    Input:
        migration - python migration file (see snuba/snuba_migrations/*/000x_*.py for examples)
        storage_set - the key of the storage-set you are writing the migration for
    Writes the given migration to a new file at the correct place in the repo
    (which is determined by storage-set key), and adds a reference to the new migration
    in the group loader (snuba/group_loader/migrations.py)
    """
    # make sure storage_set migration path exist
    path = "snuba/snuba_migrations/" + storage_set.value
    if not os.path.exists(path):
        raise ValueError(
            f"Migration path: '{path}' does not exist, perhaps '{storage_set.value}' is not a valid storage set key?"
        )

    # grab the group_loader for the storage set
    group_loader_name = (
        "".join([word.capitalize() for word in storage_set.value.split("_")]) + "Loader"
    )
    loader = getattr(group_loader, group_loader_name)()
    assert isinstance(loader, group_loader.GroupLoader)

    # get the next migration number
    existing_migrations = loader.get_migrations()
    if not existing_migrations:
        nextnum = 0
    nextnum = int(existing_migrations[-1].split("_")[0]) + 1

    # write migration to file
    newpath = f"{path}/{str(nextnum).zfill(4)}_{migration_name}.py"
    if os.path.exists(newpath):
        # this should never happen, but just in case
        raise ValueError(
            f"Error: The migration number {nextnum} was larger than the last migration in the group loader '{group_loader_name}', but the migration already exists"
        )

    from black import Mode, format_str

    with open(newpath, "w") as f:
        f.write(format_str(migration, mode=Mode()))
    return newpath
