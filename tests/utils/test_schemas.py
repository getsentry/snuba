from snuba.utils.schemas import Column, ColumnSet, Nested, String


def test_contains_nested():
    c = ColumnSet([Column("hags", Nested([("key", String()), ("value", String())]))])
    assert "hags" in c
