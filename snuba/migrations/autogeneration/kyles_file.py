# ...
def get_added_columns(
    oldstorage: str, newstorage: str
) -> list[Column[SchemaModifiers]]:
    """
    Input:
        old_storage, this is the old storage yaml in str format
        new_storage, this is the new modified storage yaml in str format

    Validates that the changes to the storage are supported, and returns the columns that were added.
    """
    oldstorage_dict = yaml.safe_load(oldstorage)
    newstorage_dict = yaml.safe_load(newstorage)

    if oldstorage_dict == newstorage_dict:
        print("storages are the same, nothing to do")
        return []

    res, reason = _only_columns_changed(oldstorage_dict, newstorage_dict)
    if not res:
        raise ValueError(
            f"""Error: expected the only change in the storage to be columns but it wasnt.
Message: {reason}"""
        )

    # columns
    oldstorage_cols = oldstorage_dict["schema"]["columns"]
    newstorage_cols = newstorage_dict["schema"]["columns"]
    left, right = 0, 0
    addedcols = []
    while left < len(oldstorage_cols) and right < len(newstorage_cols):
        if oldstorage_cols[left] == newstorage_cols[right]:
            left += 1
            right += 1
        else:
            if oldstorage_cols[left]["name"] == newstorage_cols[right]["name"]:
                raise ValueError(
                    f"""Error: column modification is unsupported, {oldstorage_cols[left]["name"]} was modified"""
                )
            elif left == 0:
                raise ValueError(
                    "Error: Adding a column to the beginning is currently unsupported, please add it anywhere else."
                )
            else:
                addedcols += parse_columns([newstorage_cols[right]])
                right += 1
    if left != len(oldstorage_cols):
        raise ValueError(
            f"Error: only column addition is currently supported, no modification or removal. Could not find a match for {oldstorage_cols[left]}"
        )
    if right < len(newstorage_cols):
        addedcols += parse_columns(newstorage_cols[right : len(newstorage_cols)])
    return addedcols


"""
def column_to_addcolumn_migration(list[Column] | Column) -> list[AddColumn] | AddColumn:
    storage_set = StorageSetKey(old_storage["storage"]["set_key"])
    newcol = parse_columns([newcols[r]])[0]
    after = None if l == len(oldcols) else oldcols[l - 1]["name"]
    col_migrations = [
        AddColumn(
            storage_set=storage_set,
            table_name=old_storage["schema"]["local_table_name"],
            column=newcol,
            after=after,
            target=OperationTarget.LOCAL,
        ),
        AddColumn(
            storage_set=storage_set,
            table_name=old_storage["schema"]["dist_table_name"],
            column=newcol,
            after=after,
            target=OperationTarget.DISTRIBUTED,
        ),
    ]
    return []
"""


def build_add_col_migrations(
    colsToAdd: Column[SchemaModifiers],
) -> tuple[list[AddColumn], list[DropColumn]]:
    """
    Given the columns to add to add to the storage,
    builds and return the forward and backwards ops as a tuple.
    """
    return ([], [])


STORAGE_YAML = "snuba/datasets/configuration/events/storages/errors.yaml"
with open(os.path.abspath(os.path.expanduser(STORAGE_YAML)), "r") as f:
    local_storage = yaml.safe_load(f)

SNUBA_REPO = "https://raw.githubusercontent.com/getsentry/snuba/master"
res = requests.get(f"{SNUBA_REPO}/{STORAGE_YAML}")
origin_storage = yaml.safe_load(res.text)

# colsToAdd = get_added_columns(origin_storage, local_storage)
# forwardops, backwardsops = build_add_col_migrations(colsToAdd)
print("hi")
