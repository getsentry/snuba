from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    Materialized,
    Nested,
    Nullable,
    String,
    WithCodecs,
    WithDefault,
)
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema


def test_required_columns() -> None:
    cols = ColumnSet(
        [
            ("col1", String()),
            ("col2", WithDefault(String(), "")),
            ("col3", Nullable(String())),
            ("col4", Array(String())),
            ("col5", Array(Nullable(String()))),
            ("col6", Nullable(Array(String()))),
            ("col7", WithCodecs(Materialized(String(), "something"), ["c"]),),
            (
                "col8",
                WithCodecs(Nullable(Materialized(String(), "something")), ["c"],),
            ),
            ("col9", Nested([("t", String())])),
        ]
    )

    schema = ReplacingMergeTreeSchema(
        cols,
        local_table_name="test",
        dist_table_name="test",
        order_by="col1",
        partition_by="col1",
        version_column="col1",
    )

    assert sorted(schema.get_required_columns()) == ["col1", "col7"]
