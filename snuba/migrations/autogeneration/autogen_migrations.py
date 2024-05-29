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
                f"Modifications to columns was invalid, column '{curr_new['name']}' was modified or reordered",
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
