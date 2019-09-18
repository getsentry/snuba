from snuba.clickhouse.columns import (
    ColumnSet,
    UInt,
    String,
    Nested,
)
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.schemas.join import (
    JoinConditionExpression,
    JoinCondition,
    JoinStructure,
    JoinedSource,
    JoinType,
)


table1 = MergeTreeSchema(
    columns=ColumnSet([
        ("t1c1", UInt(64)),
        ("t1c2", String()),
        ("t1c3", Nested([
            ("t11c4", UInt(64))
        ])),
    ]),
    local_table_name="table1",
    dist_table_name="table1",
    order_by="",
    partition_by="",
)

table2 = MergeTreeSchema(
    columns=ColumnSet([
        ("t2c1", UInt(64)),
        ("t2c2", String()),
        ("t2c3", Nested([
            ("t21c4", UInt(64))
        ])),
    ]),
    local_table_name="table2",
    dist_table_name="table2",
    order_by="",
    partition_by="",
)

table3 = MergeTreeSchema(
    columns=ColumnSet([
        ("t3c1", UInt(64)),
        ("t3c2", String()),
        ("t3c3", Nested([
            ("t31c4", UInt(64))
        ])),
    ]),
    local_table_name="table3",
    dist_table_name="table3",
    order_by="",
    partition_by="",
)

simple_join_structure = JoinStructure(
    JoinedSource("t1", table1),
    JoinedSource("t2", table2),
    [
        JoinCondition(
            left=JoinConditionExpression(table_alias="t1", column="c1"),
            right=JoinConditionExpression(table_alias="t2", column="c2"),
        ),
        JoinCondition(
            left=JoinConditionExpression(table_alias="t1", column="c3"),
            right=JoinConditionExpression(table_alias="t2", column="c4"),
        )
    ],
    JoinType.INNER
)

complex_join_structure = JoinStructure(
    JoinedSource(
        None,
        JoinStructure(
            JoinedSource("t1", table1),
            JoinedSource("t2", table2),
            [
                JoinCondition(
                    left=JoinConditionExpression(table_alias="t1", column="c1"),
                    right=JoinConditionExpression(table_alias="t2", column="c2"),
                ),
            ],
            JoinType.FULL
        ),
    ),
    JoinedSource("t3", table3),
    [
        JoinCondition(
            left=JoinConditionExpression(table_alias="t1", column="c1"),
            right=JoinConditionExpression(table_alias="t3", column="c3"),
        ),
    ],
    JoinType.INNER
)
