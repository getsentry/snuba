import pytest

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.autogeneration.autogen_migrations import generate_migration_ops
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.operations import AddColumn, DropColumn, OperationTarget
from snuba.utils.schemas import Column, DateTime, UInt


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
local_table_name: errors_local
dist_table_name: errors_dist
"""


def test_add_column() -> None:
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
        "{ name: newcol2, type: UInt, args: { schema_modifiers: [nullable], size: 8 } }",
    ]
    forwardops, backwardsops = generate_migration_ops(
        mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols)
    )
    expected_forward = [
        AddColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_local",
            column=Column("newcol1", DateTime()),
            after="timestamp",
            target=OperationTarget.LOCAL,
        ),
        AddColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_dist",
            column=Column("newcol1", DateTime()),
            after="timestamp",
            target=OperationTarget.DISTRIBUTED,
        ),
        AddColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_local",
            column=Column(
                "newcol2", UInt(size=8, modifiers=MigrationModifiers(nullable=True))
            ),
            after="event_id",
            target=OperationTarget.LOCAL,
        ),
        AddColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_dist",
            column=Column(
                "newcol2", UInt(size=8, modifiers=MigrationModifiers(nullable=True))
            ),
            after="event_id",
            target=OperationTarget.DISTRIBUTED,
        ),
    ]
    expected_backwards = [
        DropColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_dist",
            column_name="newcol2",
            target=OperationTarget.DISTRIBUTED,
        ),
        DropColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_local",
            column_name="newcol2",
            target=OperationTarget.LOCAL,
        ),
        DropColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_dist",
            column_name="newcol1",
            target=OperationTarget.DISTRIBUTED,
        ),
        DropColumn(
            storage_set=StorageSetKey("events"),
            table_name="errors_local",
            column_name="newcol1",
            target=OperationTarget.LOCAL,
        ),
    ]
    assert forwardops == expected_forward and backwardsops == expected_backwards


def test_modify_column() -> None:
    cols = [
        "{ name: timestamp, type: DateTime }",
    ]
    new_cols = [
        "{ name: timestamp, type: UUID }",
    ]
    with pytest.raises(ValueError):
        generate_migration_ops(
            mockstoragewithcolumns(cols),
            mockstoragewithcolumns(new_cols),
        )


def test_reorder_columns() -> None:
    cols = [
        "{ name: project_id, type: UInt, args: { size: 64 } }",
        "{ name: timestamp, type: DateTime }",
    ]
    new_cols = [
        "{ name: timestamp, type: DateTime }",
        "{ name: project_id, type: UInt, args: { size: 64 } }",
    ]
    with pytest.raises(ValueError):
        generate_migration_ops(
            mockstoragewithcolumns(cols),
            mockstoragewithcolumns(new_cols),
        )


def test_delete_column() -> None:
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
    with pytest.raises(ValueError):
        generate_migration_ops(
            mockstoragewithcolumns(cols),
            mockstoragewithcolumns(new_cols),
        )
