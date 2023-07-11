from typing import List, Sequence

from snuba.clickhouse.columns import Column, DateTime, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    # Group
    Column("group_status", UInt(8)),
    Column("group_substatus", UInt(8, Modifiers(nullable=True))),
    Column("group_first_seen", DateTime()),
    Column("group_num_comments", UInt(64)),
    # GroupAssignee
    Column("assignee_user_id", UInt(64, Modifiers(nullable=True))),
    Column("assignee_team_id", UInt(64, Modifiers(nullable=True))),
    # GroupOwner
    Column("owner_suspect_commit_user_id", UInt(64, Modifiers(nullable=True))),
    Column("owner_ownership_rule_user_id", UInt(64, Modifiers(nullable=True))),
    Column("owner_ownership_rule_team_id", UInt(64, Modifiers(nullable=True))),
    Column("owner_codeowners_user_id", UInt(64, Modifiers(nullable=True))),
    Column("owner_codeowners_team_id", UInt(64, Modifiers(nullable=True))),
    # meta
    Column("deleted", UInt(8)),
    Column("message_timestamp", DateTime()),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(project_id, group_id)",
                    version_column="deleted",
                    settings={"index_granularity": "8192"},
                    storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="group_attributes_dist",
                    sharding_key="cityHash64(group_id)",
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name=params[0],
                target=params[1],
            )
            for params in [
                ("group_attributes_dist", OperationTarget.DISTRIBUTED),
                ("group_attributes_local", OperationTarget.LOCAL),
            ]
        ]
