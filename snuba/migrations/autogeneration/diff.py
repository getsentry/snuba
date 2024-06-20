import os
from typing import Any, Sequence, cast

import yaml
from black import Mode, format_str  # type: ignore

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.configuration.utils import parse_columns
from snuba.migrations import group_loader
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.operations import AddColumn, DropColumn, OperationTarget
from snuba.utils.schemas import Column, ColumnType, SchemaModifiers

"""
This file is for autogenerating the migration for adding a column to your storage.
"""


def generate_migration(oldstorage: str, newstorage: str) -> None:
    old_stor_dict = yaml.safe_load(oldstorage)
    new_stor_dict = yaml.safe_load(newstorage)
    forwards, backwards = storage_diff_to_migration_ops(old_stor_dict, new_stor_dict)
    migration = _migration_ops_to_migration(forwards, backwards)
    _write_migration(migration, StorageSetKey(new_stor_dict["storage"]["set_key"]))


def storage_diff_to_migration_ops(
    oldstorage: dict[str, Any], newstorage: dict[str, Any]
) -> tuple[list[AddColumn], list[DropColumn]]:
    """
    Input:
        old_storage, the original storage yaml in yaml.safe_load format
        new_storage, the modified storage yaml in yaml.safe_load format

    Returns a tuple (forwardops, backwardsops) this are the forward and backward migration
    operations required to migrate the storage as described in the given yaml files.

    Only supports adding columns, throws error for anything else.
    """
    valid, reason = _is_valid_add_column(oldstorage, newstorage)
    if not valid:
        raise ValueError(reason)

    oldcol_names = set(col["name"] for col in oldstorage["schema"]["columns"])
    newcols = newstorage["schema"]["columns"]

    forwardops: list[AddColumn] = []
    for i, col in enumerate(newcols):
        if col["name"] not in oldcol_names:
            column = _schema_column_to_migration_column(parse_columns([col])[0])
            after = newcols[i - 1]["name"]
            storage_set = StorageSetKey(newstorage["storage"]["set_key"])
            forwardops += [
                AddColumn(
                    storage_set=storage_set,
                    table_name=newstorage["schema"]["local_table_name"],
                    column=column,
                    after=after,
                    target=OperationTarget.LOCAL,
                ),
                AddColumn(
                    storage_set=storage_set,
                    table_name=newstorage["schema"]["dist_table_name"],
                    column=column,
                    after=after,
                    target=OperationTarget.DISTRIBUTED,
                ),
            ]
    return (forwardops, [op.get_reverse() for op in reversed(forwardops)])


def _migration_ops_to_migration(
    forwards_ops: Sequence[AddColumn],
    backwards_ops: Sequence[DropColumn],
) -> str:
    """
    Given a lists of forward and backwards ops, returns a python class
    definition for the migration as a str. The migration must be non-blocking.
    """

    forwards_str = (
        "[" + ", ".join([f"operations.{repr(op)}" for op in forwards_ops]) + "]"
    )
    backwards_str = (
        "[" + ", ".join([f"operations.{repr(op)}" for op in backwards_ops]) + "]"
    )

    return f"""
from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import operations
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import OperationTarget
from snuba.utils import schemas
from snuba.utils.schemas import Column

class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return {forwards_str}

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return {backwards_str}
"""


def _write_migration(migration: str, storage_set: StorageSetKey) -> None:
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
    newpath = f"{path}/{str(nextnum).zfill(4)}_generated_migration.py"
    if os.path.exists(newpath):
        # this should never happen, but just in case
        raise ValueError(
            f"Error: The migration number {nextnum} was larger than the last migration in the group loader '{group_loader_name}', but the migration already exists"
        )
    with open(newpath, "w") as f:
        f.write(format_str(migration, mode=Mode()))


def _is_valid_add_column(
    oldstorage: dict[str, Any], newstorage: dict[str, Any]
) -> tuple[bool, str]:
    """
    Input:
        old_storage, the old storage yaml in yaml.safe_load format
        new_storage, the new (modified) storage yaml in yaml.safe_load format

    Returns true if the changes to the storage is valid column addition, false otherwise,
    along with a reasoning.
    """
    if oldstorage == newstorage:
        return True, "storages are the same"

    # verify nothing changed but the columns
    t1 = oldstorage["schema"].pop("columns")
    t2 = newstorage["schema"].pop("columns")
    if not (oldstorage == newstorage):
        return (
            False,
            "Expected the only change to the storage to be the columns, but that is not true",
        )
    oldstorage["schema"]["columns"] = t1
    newstorage["schema"]["columns"] = t2

    # verify only changes to columns is additions
    oldstorage_cols = oldstorage["schema"]["columns"]
    newstorage_cols = newstorage["schema"]["columns"]

    colnames_old = set(e["name"] for e in oldstorage_cols)
    colnames_new = set(e["name"] for e in newstorage_cols)
    if not colnames_old.issubset(colnames_new):
        return (False, "Column removal is not supported")

    pold, pnew = 0, 0
    while pold < len(oldstorage_cols) and pnew < len(newstorage_cols):
        curr_old = oldstorage_cols[pold]
        curr_new = newstorage_cols[pnew]

        if curr_old == curr_new:
            pold += 1
            pnew += 1
        elif curr_new["name"] in colnames_old:
            return (
                False,
                f"Modification to columns in unsupported, column '{curr_new['name']}' was modified or reordered",
            )
        else:
            if pold == 0:
                return (
                    False,
                    "Adding a column to the beginning is currently unsupported, please add it anywhere else.",
                )
            else:
                pnew += 1
    assert pold == len(oldstorage_cols)  # should always hold
    return True, ""


def _schema_column_to_migration_column(
    column: Column[SchemaModifiers],
) -> Column[MigrationModifiers]:
    """
    Given SchemaModifiers returns equivalent MigrationModifiers.
    Only nullable is supported, throws error if conversion cant be made.
    """
    newtype = cast(ColumnType[MigrationModifiers], column.type.get_raw())
    mods = column.type.get_modifiers()
    if not mods:
        return Column(column.name, newtype)

    # convert schema modifiers to migration modifiers
    if mods.readonly:
        raise ValueError("readonly modifier is not supported")
    newtype = newtype.set_modifiers(MigrationModifiers(nullable=mods.nullable))
    return Column(column.name, newtype)
