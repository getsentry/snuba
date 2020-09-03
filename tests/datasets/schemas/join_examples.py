from snuba.clickhouse.columns import (
    ColumnSet,
    UInt,
    String,
    Nested,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.schemas.join import (
    JoinConditionExpression,
    JoinCondition,
    JoinClause,
    JoinType,
    TableJoinNode,
)


table1 = MergeTreeSchema(
    columns=ColumnSet(
        [
            ("t1c1", UInt(64)),
            ("t1c2", String()),
            ("t1c3", Nested([("t11c4", UInt(64))])),
        ]
    ),
    local_table_name="table1",
    dist_table_name="table1",
    storage_set_key=StorageSetKey.EVENTS,
    order_by=[],
    partition_by="",
).get_data_source()

table2 = MergeTreeSchema(
    columns=ColumnSet(
        [
            ("t2c1", UInt(64)),
            ("t2c2", String()),
            ("t2c3", Nested([("t21c4", UInt(64))])),
        ]
    ),
    local_table_name="table2",
    dist_table_name="table2",
    storage_set_key=StorageSetKey.EVENTS,
    order_by=[],
    partition_by="",
).get_data_source()

table3 = MergeTreeSchema(
    columns=ColumnSet(
        [
            ("t3c1", UInt(64)),
            ("t3c2", String()),
            ("t3c3", Nested([("t31c4", UInt(64))])),
        ]
    ),
    local_table_name="table3",
    dist_table_name="table3",
    storage_set_key=StorageSetKey.EVENTS,
    order_by=[],
    partition_by="",
).get_data_source()


simple_join_structure = JoinClause(
    TableJoinNode(table1.format_from(), table1.get_columns(), [], [], [], "t1"),
    TableJoinNode(table2.format_from(), table2.get_columns(), [], [], [], "t2"),
    [
        JoinCondition(
            left=JoinConditionExpression(table_alias="t1", column="t1c1"),
            right=JoinConditionExpression(table_alias="t2", column="t2c2"),
        ),
        JoinCondition(
            left=JoinConditionExpression(table_alias="t1", column="t1c3"),
            right=JoinConditionExpression(table_alias="t2", column="t2c4"),
        ),
    ],
    JoinType.INNER,
)

complex_join_structure = JoinClause(
    JoinClause(
        TableJoinNode(table1.format_from(), table1.get_columns(), [], [], [], "t1"),
        TableJoinNode(table2.format_from(), table2.get_columns(), [], [], [], "t2"),
        [
            JoinCondition(
                left=JoinConditionExpression(table_alias="t1", column="t1c1"),
                right=JoinConditionExpression(table_alias="t2", column="t2c2"),
            ),
        ],
        JoinType.FULL,
    ),
    TableJoinNode(table3.format_from(), table3.get_columns(), [], [], [], "t3"),
    [
        JoinCondition(
            left=JoinConditionExpression(table_alias="t1", column="t1c1"),
            right=JoinConditionExpression(table_alias="t3", column="t3c3"),
        ),
    ],
    JoinType.INNER,
)
