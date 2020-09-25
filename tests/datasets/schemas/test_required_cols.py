from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    Materialized,
    NullableOld as Nullable,
    String,
    WithCodecs,
    WithDefault,
)


def test_modifiers() -> None:
    cols = ColumnSet(
        [
            ("col1", WithDefault(String(), "")),
            ("col2", Nullable(Array(String()))),
            ("col3", WithCodecs(Materialized(String(), "something"), ["c"]),),
            (
                "col4",
                WithCodecs(Nullable(Materialized(String(), "something")), ["c"],),
            ),
        ]
    )

    assert [WithDefault] == cols["col1"].type.get_all_modifiers()
    assert [Nullable] == cols["col2"].type.get_all_modifiers()
    assert [Materialized, WithCodecs] == cols["col3"].type.get_all_modifiers()
    assert [Materialized, Nullable, WithCodecs] == cols["col4"].type.get_all_modifiers()
