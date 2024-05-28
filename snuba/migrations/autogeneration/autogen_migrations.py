import yaml

from snuba.datasets.configuration.storage_builder import build_storage_from_yaml_dict

"""
This file is for autogenerating the migration for adding a column to your storage.
"""


def is_valid_add_column(oldstorage: str, newstorage: str) -> tuple[bool, str]:
    """
    Input:
        old_storage, the old storage yaml in str format
        new_storage, the modified storage yaml in str format

    Returns true if the changes to the storage is valid column addition, false otherwise,
    along with a reasoning.
    """
    oldstorage_dict = yaml.safe_load(oldstorage)
    newstorage_dict = yaml.safe_load(newstorage)

    if oldstorage_dict == newstorage_dict:
        return True, "storages are the same"

    # ensure both storages are valid
    build_storage_from_yaml_dict(oldstorage_dict)
    build_storage_from_yaml_dict(newstorage_dict)

    # make sure the columns field is the only thing that changed
    if not (
        oldstorage_dict["schema"].pop("columns")
        == newstorage_dict["schema"].pop("columns")
    ):
        return (
            False,
            "Expected the only change to the storage to be the columns, but that is not true",
        )

    # make sure the changes to columns reflect valid addition
    oldstorage_cols = oldstorage_dict["schema"]["columns"]
    newstorage_cols = newstorage_dict["schema"]["columns"]

    colnames_old = set(e["name"] for e in oldstorage_cols)
    colsnames_new = set(e["name"] for e in newstorage_cols)
    if not colnames_old.issubset(colsnames_new):
        return (
            False,
            f"Column removal is unsupported, the following columns were present in the old storage but not the new one:\n{colnames_old-colsnames_new}",
        )

    pold, pnew = 0, 0
    while pold < len(oldstorage_cols) and pnew < len(newstorage_cols):
        curr_old = oldstorage_cols[pold]
        curr_new = newstorage_cols[pnew]

        if curr_old == curr_new:
            pold += 1
            pnew += 1
        else:
            if curr_old["name"] == curr_new["name"]:
                return (
                    False,
                    f"Column modification is unsupported, column '{oldstorage_cols[pold]['name']}' was modified",
                )
            elif curr_new["name"] in colnames_old:
                return (
                    False,
                    f"Reordering of existing columns is unsupported, found column '{curr_new['name']}' in a new position",
                )
            elif pold == 0:
                return (
                    False,
                    "Adding a column to the beginning is currently unsupported, please add it anywhere else.",
                )
            else:
                pnew += 1
    assert pold == len(oldstorage_cols)  # should always hold
    return True, ""


def dict_diff(d1: dict, d2: dict) -> dict:
    diff = {
        "in_d1_not_d2": set(),
        "in_d2_not_d1": set(),
        "different_values": {},
    }

    s1 = set(d1.keys())
    s2 = set(d2.keys())
    diff["in_d1_not_d2"] = s1 - s2
    diff["in_d2_not_d1"] = s2 - s1

    for key in d1:
        if key in d2 and d1[key] != d2[key]:
            diff["different_values"][key] = {"d1": d1[key], "d2": d2[key]}

    return diff


def _only_columns_changed(old_storage: dict, new_storage: dict) -> tuple[bool, str]:
    if old_storage == new_storage:
        return True, "storages are the exact same"
    # validate yamls
    diff = dict_diff(old_storage, new_storage)
    if diff["in_d1_not_d2"] or diff["in_d2_not_d1"]:
        return (
            False,
            f"""
Old and new storages to have different key sets:
in_old_not_new: {diff["in_d1_not_d2"]}
in_new_not_old: {diff["in_d2_not_d1"]}
""",
        )
    assert diff["different_values"]  # otherwise they are the exact same
    if (
        len(diff["different_values"].keys()) > 1
        or list(diff["different_values"].keys())[0] != "schema"
    ):
        return (
            False,
            f"expected only schema field to change, but got {diff['different_values'].keys()}",
        )
    # validate schemas
    schema_diff = dict_diff(
        diff["different_values"]["schema"]["d1"],
        diff["different_values"]["schema"]["d2"],
    )
    if schema_diff["in_d1_not_d2"] or schema_diff["in_d2_not_d1"]:
        return (
            False,
            f"""
Old and new schemas have different keysets
in_old_not_new: {schema_diff["in_d1_not_d2"]}
in_new_not_old: {schema_diff["in_d2_not_d1"]}
""",
        )
    assert schema_diff["different_values"]  # otherwise exact same
    if (
        len(schema_diff["different_values"].keys()) > 1
        or list(schema_diff["different_values"].keys())[0] != "columns"
    ):
        return (
            False,
            f"Expected the only changed field to be columns, but got {schema_diff['different_values'].keys()}",
        )
    return True, ""
