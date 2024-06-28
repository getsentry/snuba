import pytest

from snuba.migrations.autogeneration.diff import generate_migration


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
    generate_migration(mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols))
    """
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
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column("newcol1", schemas.DateTime(modifiers=None)),
                after="timestamp",
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column("newcol1", schemas.DateTime(modifiers=None)),
                after="timestamp",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column(
                    "newcol2",
                    schemas.UInt(
                        8,
                        modifiers=MigrationModifiers(
                            nullable=True,
                            low_cardinality=False,
                            default=None,
                            materialized=None,
                            codecs=None,
                            ttl=None,
                        ),
                    ),
                ),
                after="event_id",
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column(
                    "newcol2",
                    schemas.UInt(
                        8,
                        modifiers=MigrationModifiers(
                            nullable=True,
                            low_cardinality=False,
                            default=None,
                            materialized=None,
                            codecs=None,
                            ttl=None,
                        ),
                    ),
                ),
                after="event_id",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column_name="newcol2",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column_name="newcol2",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column_name="newcol1",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column_name="newcol1",
                target=OperationTarget.LOCAL,
            ),
        ]
"""
    # assert format_str(migration, mode=Mode()) == format_str(
    #    expected_migration, mode=Mode()
    # )


def test_modify_column() -> None:
    cols = [
        "{ name: timestamp, type: DateTime }",
    ]
    new_cols = [
        "{ name: timestamp, type: UUID }",
    ]
    with pytest.raises(
        ValueError,
        match="Modification to columns in unsupported, column 'timestamp' was modified or reordered",
    ):
        generate_migration(
            mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols)
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
    with pytest.raises(
        ValueError,
        match="Modification to columns in unsupported, column 'timestamp' was modified or reordered",
    ):
        generate_migration(
            mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols)
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
    with pytest.raises(ValueError, match="Column removal is not supported"):
        generate_migration(
            mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols)
        )
