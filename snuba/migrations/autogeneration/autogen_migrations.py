import os

import requests
import yaml

from snuba.clickhouse.columns import Column, SchemaModifiers
from snuba.datasets.configuration.utils import parse_columns

"""
This file is for autogenerating the migration for adding a column to your storage.
"""


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


def get_new_columns(
    old_storage: dict, new_storage: dict
) -> list[Column[SchemaModifiers]]:
    """
    Input
        old_storage, this is a dictionary representation of a storage yaml file from yaml.safe_load
            it represents the current state of the storage pre-migration
        new_storage, similar to old_storage but this represents the new storage post-migration


    Validates that the changes to the storage are supported, and returns the new columns
    to be added by a migration.
    """
    if old_storage == new_storage:
        print("storages are the same, nothing to do")
        return []
    # validate yamls
    diff = dict_diff(old_storage, new_storage)
    if diff["in_d1_not_d2"] != set() or diff["in_d2_not_d1"] != set():
        err_msg = f"""
Error: expected old and new storages to have the same key set but they didnt.
in_d1_not_d2: {diff["in_d1_not_d2"]}
in_d2_not_d1: {diff["in_d2_not_d1"]}
"""
        raise ValueError(err_msg)

    assert diff["different_values"]
    if (
        len(diff["different_values"].keys()) > 1
        or list(diff["different_values"].keys())[0] != "schema"
    ):
        raise ValueError(
            f"Error: expected the only changed field to be schema, but it was {diff['different_values'].keys()}"
        )
    # validate schemas
    schema_diff = dict_diff(
        diff["different_values"]["schema"]["d1"],
        diff["different_values"]["schema"]["d2"],
    )
    if not (
        schema_diff["in_d1_not_d2"] == set() and schema_diff["in_d2_not_d1"] == set()
    ):
        err_msg = f"""
Error: expected old and new schemas to have the same key set but they didnt.
in_d1_not_d2: {schema_diff["in_d1_not_d2"]}
in_d2_not_d1: {schema_diff["in_d2_not_d1"]}
"""
        raise ValueError(err_msg)
    assert schema_diff["different_values"]
    if (
        len(schema_diff["different_values"].keys()) > 1
        or list(schema_diff["different_values"].keys())[0] != "columns"
    ):
        raise ValueError(
            f"Error: expected the only changed field to be column, but it was {schema_diff['different_values'].keys()}"
        )
    # columns
    oldcols = {}
    for e in schema_diff["different_values"]["columns"]["d1"]:
        if e["name"] in oldcols:
            raise ValueError(
                f"Error: duplicate column name \"{e['name']}\" in oldstorage"
            )
        oldcols[e["name"]] = e
    newcols = {}
    for e in schema_diff["different_values"]["columns"]["d2"]:
        if e["name"] in newcols:
            raise ValueError(
                f"Error: duplicate column name \"{e['name']}\" in new storage"
            )
        newcols[e["name"]] = e

    col_diff = dict_diff(oldcols, newcols)
    if col_diff["different_values"]:
        raise ValueError(
            f"""Error: column modification unsupported, only column addition is
schema.columns different_values: {schema_diff['different_values']['columns']}
"""
        )
    if col_diff["in_d1_not_d2"]:
        raise ValueError(
            f"""
Error: column removal unsupported, only column addition
in_old_not_new: {col_diff["in_d1_not_d2"]}
"""
        )
    assert col_diff["in_d2_not_d1"]
    return parse_columns([newcols[k] for k in col_diff["in_d2_not_d1"]])


STORAGE_YAML = "snuba/datasets/configuration/events/storages/errors.yaml"
with open(os.path.abspath(os.path.expanduser(STORAGE_YAML)), "r") as f:
    local_storage = yaml.safe_load(f)

SNUBA_REPO = "https://raw.githubusercontent.com/getsentry/snuba/master"
res = requests.get(f"{SNUBA_REPO}/{STORAGE_YAML}")
origin_storage = yaml.safe_load(res.text)

colsToAdd = get_new_columns(origin_storage, local_storage)
print("hi")
