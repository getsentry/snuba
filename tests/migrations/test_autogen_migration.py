from snuba.migrations.autogeneration.autogen_migrations import is_valid_add_column


def mockstoragewithcolumns(cols: list[str]) -> str:
    colstr = ",\n            ".join([s for s in cols])
    return f"""
version: v1
kind: writable_storage
name: errors
storage:
    key: errors
    set_key: events
readiness_state: complete
schema:
    columns:
        [
            {colstr}
        ]
    local_table_name: errors_local
    dist_table_name: errors_dist
    partition_format:
        - retention_days
        - date
    not_deleted_mandatory_condition: deleted
"""


def test_is_valid_add_column_basic() -> None:
    cols = [
        "{ name: project_id, type: UInt, args: { size: 64 } }",
        "{ name: timestamp, type: DateTime }",
        "{ name: event_id, type: UUID }",
    ]
    new_cols = [
        "{ name: project_id, type: UInt, args: { size: 64 } }",
        "{ name: timestamp, type: DateTime }",
        "{ name: newcol1, type: DateTime }",
        "{ name: event_id, type: UUID }",
        "{ name: newcol2, type: UInt, args: { schema_modifiers: [readonly], size: 8 } }",
    ]
    res, _ = is_valid_add_column(
        mockstoragewithcolumns(cols),
        mockstoragewithcolumns(new_cols),
    )
    assert res


def test_is_valid_add_column_modify() -> None:
    cols = [
        "{ name: timestamp, type: DateTime }",
    ]
    new_cols = [
        "{ name: timestamp, type: UUID }",
    ]
    res, _ = is_valid_add_column(
        mockstoragewithcolumns(cols),
        mockstoragewithcolumns(new_cols),
    )
    assert not res


def test_is_valid_add_column_reorder() -> None:
    cols = [
        "{ name: project_id, type: UInt, args: { size: 64 } }",
        "{ name: timestamp, type: DateTime }",
    ]
    new_cols = [
        "{ name: timestamp, type: DateTime }",
        "{ name: project_id, type: UInt, args: { size: 64 } }",
    ]
    res, _ = is_valid_add_column(
        mockstoragewithcolumns(cols),
        mockstoragewithcolumns(new_cols),
    )
    assert not res


def test_is_valid_add_column_delete() -> None:
    cols = [
        "{ name: project_id, type: UInt, args: { size: 64 } }",
        "{ name: timestamp, type: DateTime }",
        "{ name: event_id, type: UUID }",
    ]
    new_cols = [
        "{ name: project_id, type: UInt, args: { size: 64 } }",
        "{ name: timestamp, type: DateTime }",
        "{ name: newcol1, type: DateTime }",
    ]
    res, _ = is_valid_add_column(
        mockstoragewithcolumns(cols),
        mockstoragewithcolumns(new_cols),
    )
    assert not res
