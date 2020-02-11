import pytest

from snuba.datasets.promoted_columns import PromotedColumnSpec
from tests.datasets.schemas.join_examples import (
    simple_join_structure,
    complex_join_structure,
)

test_data = [
    (
        simple_join_structure,
        "test_table1 t1 INNER JOIN test_table2 t2 ON t1.t1c1 = t2.t2c2 AND t1.t1c3 = t2.t2c4",
    ),
    (
        complex_join_structure,
        "test_table1 t1 FULL JOIN test_table2 t2 ON t1.t1c1 = t2.t2c2 "
        "INNER JOIN test_table3 t3 ON t1.t1c1 = t3.t3c3",
    ),
]


@pytest.mark.parametrize("structure, expected", test_data)
def test_join_source(structure, expected):
    assert structure.format_from() == expected


def test_promoted_spec():
    assert simple_join_structure.get_promoted_columns_spec() == {
        "t1.t1c3": PromotedColumnSpec({"tag": "t1.t1c1"}),
        "t2.t2c3": PromotedColumnSpec({"tag": "t2.t2c1"}),
    }
