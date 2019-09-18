import pytest

from tests.datasets.schemas.join_examples import simple_join_structure, complex_join_structure

test_data = [
    (
        simple_join_structure,
        "test_table1 t1 INNER JOIN test_table2 t2 ON t1.c1 = t2.c2 AND t1.c3 = t2.c4"
    ),
    (
        complex_join_structure,
        "test_table1 t1 FULL JOIN test_table2 t2 ON t1.c1 = t2.c2 "
        " INNER JOIN test_table3 t3 ON t1.c1 = t3.c3"
    )
]


@pytest.mark.parametrize("structure, expected", test_data)
def test_join_source(structure, expected):
    join_clause = structure.get_clickhouse_source()
    assert join_clause == expected
