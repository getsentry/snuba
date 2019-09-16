import pytest

from snuba.datasets.schemas.table_schemas import MergeTreeSchema
from snuba.datasets.schemas.join_schema import (
    JoinMapping,
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
            JoinedSource(table1, "t1"),
            JoinedSource(table2, "t2"),
            [
                JoinMapping(
                    left_alias="t1",
                    left_column="c1",
                    right_alias="t2",
                    right_column="c2",
                ),
                JoinMapping(
                    left_alias="t1",
                    left_column="c3",
                    right_alias="t2",
                    right_column="c4",
                )
            ],
            JoinType.INNER
        ),
        "(test_table1 t1 INNER JOIN test_table2 t2 ON t1.c1 = t2.c2 AND t1.c3 = t2.c4)"
    ),
    (
        JoinStructure(
            JoinedSource(
                JoinStructure(
                    JoinedSource(table1, "t1"),
                    JoinedSource(table2, "t2"),
                    [
                        JoinMapping(
                            left_alias="t1",
                            left_column="c1",
                            right_alias="t2",
                            right_column="c2",
                        ),
                    ],
                    JoinType.FULL
                ),
                None,
            ),
            JoinedSource(table3, "t3"),
            [
                JoinMapping(
                    left_alias="t1",
                    left_column="c1",
                    right_alias="t3",
                    right_column="c3",
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
    join_clause = structure.get_from_clause()
    assert join_clause == expected
