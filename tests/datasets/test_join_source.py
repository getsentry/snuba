import pytest

from snuba.datasets.schema_source import (
    JoinMapping,
    JoinSchemaSource,
    JoinedSource,
    JoinType,
    TableSchemaSource,
)

test_data = [
    (
        JoinSchemaSource(
            JoinedSource(TableSchemaSource("table1"), "t1"),
            JoinedSource(TableSchemaSource("table2"), "t2"),
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
        "(table1 t1 INNER JOIN table2 t2 ON t1.c1 = t2.c2 AND t1.c3 = t2.c4)"
    ),
    (
        JoinSchemaSource(
            JoinedSource(
                JoinSchemaSource(
                    JoinedSource(TableSchemaSource("table1"), "t1"),
                    JoinedSource(TableSchemaSource("table2"), "t2"),
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
            JoinedSource(TableSchemaSource("table3"), "t3"),
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
        "((table1 t1 FULL JOIN table2 t2 ON t1.c1 = t2.c2) "
        " INNER JOIN table3 t3 ON t1.c1 = t3.c3)"
    )
]


@pytest.mark.parametrize("structure, expected", test_data)
def test_join_source(structure, expected):
    join_clause = structure.for_query()
    assert join_clause == expected
