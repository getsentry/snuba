import pytest

from snuba.datasets.schema_storage import (
    JoinMapping,
    JoinSchemaStorage,
    JoinedSource,
    JoinType,
    TableSchemaStorage,
)

test_data = [
    (
        JoinSchemaStorage(
            JoinedSource(TableSchemaStorage("table1", "table1"), "t1"),
            JoinedSource(TableSchemaStorage("table2", "table2"), "t2"),
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
        JoinSchemaStorage(
            JoinedSource(
                JoinSchemaStorage(
                    JoinedSource(TableSchemaStorage("table1", "table1"), "t1"),
                    JoinedSource(TableSchemaStorage("table2", "table2"), "t2"),
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
            JoinedSource(TableSchemaStorage("table3", "table3"), "t3"),
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
    join_clause = structure.for_query()
    assert join_clause == expected
