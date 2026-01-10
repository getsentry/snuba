import ast
from typing import Any

import pytest
import yaml

from snuba.migrations.autogeneration.diff import generate_python_migration


def mockstoragewithcolumns(cols: list[str]) -> Any:
    colstr = ",\n            ".join([s for s in cols])
    storage = f"""
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
    return yaml.safe_load(storage)


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
    expected_migration = """
from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import AddColumn, DropColumn, OperationTarget, SqlOperation
from snuba.utils import schemas
from snuba.utils.schemas import Column


class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column=Column("newcol1", schemas.DateTime(modifiers=None)),
                after="timestamp",
                target=OperationTarget.LOCAL,
            ),
            AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column("newcol1", schemas.DateTime(modifiers=None)),
                after="timestamp",
                target=OperationTarget.DISTRIBUTED,
            ),
            AddColumn(
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
            AddColumn(
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

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column_name="newcol2",
                target=OperationTarget.DISTRIBUTED,
            ),
            DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column_name="newcol2",
                target=OperationTarget.LOCAL,
            ),
            DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column_name="newcol1",
                target=OperationTarget.DISTRIBUTED,
            ),
            DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                column_name="newcol1",
                target=OperationTarget.LOCAL,
            ),
        ]
"""
    migration = generate_python_migration(
        mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols)
    )
    assert ast.dump(ast.parse(migration)) == ast.dump(ast.parse(expected_migration))


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
        generate_python_migration(mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols))


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
        generate_python_migration(mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols))


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
        generate_python_migration(mockstoragewithcolumns(cols), mockstoragewithcolumns(new_cols))
