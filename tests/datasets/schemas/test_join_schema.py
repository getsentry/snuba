from snuba.clickhouse.columns import (
    ColumnSet,
    UInt,
    String,
    Nested,
)

# from snuba.query.data_source.join import JoinedSchema
# from tests.datasets.schemas.join_examples import complex_join_structure


def test_joined_columns():
    schema = JoinedSchema(complex_join_structure)
    columns = schema.get_columns()

    expected_columns = ColumnSet(
        [
            ("t1.t1c1", UInt(64)),
            ("t1.t1c2", String()),
            ("t1.t1c3", Nested([("t11c4", UInt(64))])),
            ("t2.t2c1", UInt(64)),
            ("t2.t2c2", String()),
            ("t2.t2c3", Nested([("t21c4", UInt(64))])),
            ("t3.t3c1", UInt(64)),
            ("t3.t3c2", String()),
            ("t3.t3c3", Nested([("t31c4", UInt(64))])),
        ]
    )

    # Checks equality between flattened columns. Nested columns are
    # exploded here
    assert set([c.flattened for c in columns]) == set(
        [c.flattened for c in expected_columns]
    )

    # Checks equality between the structured set of columns. Nested columns
    # are not exploded.
    assert set([repr(c) for c in columns.columns]) == set(
        [repr(c) for c in expected_columns.columns]
    )
