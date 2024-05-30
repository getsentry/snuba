import yaml

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

    # nothing changed but the columns
    t1 = oldstorage_dict["schema"].pop("columns")
    t2 = newstorage_dict["schema"].pop("columns")
    if not (oldstorage_dict == newstorage_dict):
        return (
            False,
            "Expected the only change to the storage to be the columns, but that is not true",
        )
    oldstorage_dict["schema"]["columns"] = t1
    newstorage_dict["schema"]["columns"] = t2

    # only changes to columns is additions
    oldstorage_cols = oldstorage_dict["schema"]["columns"]
    newstorage_cols = newstorage_dict["schema"]["columns"]

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
