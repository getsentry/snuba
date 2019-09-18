import pytest

from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.schemas.join import (
    JoinConditionExpression,
    JoinCondition,
    JoinStructure,
    JoinedSource,
    JoinType,
)


table1 = MergeTreeSchema(
    columns=None,
    local_table_name="table1",
    dist_table_name="table1",
    order_by="",
    partition_by="",
)

table2 = MergeTreeSchema(
    columns=None,
    local_table_name="table2",
    dist_table_name="table2",
    order_by="",
    partition_by="",
)

table3 = MergeTreeSchema(
    columns=None,
    local_table_name="table3",
    dist_table_name="table3",
    order_by="",
    partition_by="",
)

test_data = [
    (
        JoinStructure(
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
        ),
        "(test_table1 t1 INNER JOIN test_table2 t2 ON t1.c1 = t2.c2 AND t1.c3 = t2.c4)"
    ),
    (
        JoinStructure(
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
        ),
        "((test_table1 t1 FULL JOIN test_table2 t2 ON t1.c1 = t2.c2) "
        " INNER JOIN test_table3 t3 ON t1.c1 = t3.c3)"
    )
]


@pytest.mark.parametrize("structure, expected", test_data)
def test_join_source(structure, expected):
    join_clause = structure.get_clickhouse_source()
    assert join_clause == expected
