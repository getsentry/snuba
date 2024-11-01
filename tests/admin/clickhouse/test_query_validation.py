import pytest

from snuba.admin.clickhouse.common import InvalidCustomQuery, validate_ro_query


def test_select_query() -> None:
    validate_ro_query("SELECT * FROM my_table")
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("INSERT INTO my_table (col) VALUES ('value')")


def test_multiple_queries() -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("SELECT * FROM my_table; SELECT * FROM other_table")


def test_allowed_tables() -> None:
    validate_ro_query(
        "SELECT * FROM my_table, other_table",
        allowed_tables={"my_table", "other_table"},
    )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, other_table",
            allowed_tables={"my_table"},
        )


def test_allowed_tables_with_array_join() -> None:
    validate_ro_query(
        "SELECT * FROM my_table ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
        allowed_tables={"my_table"},
    )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, other_table ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
            allowed_tables={"my_table"},
        )


def test_allowed_tables_with_left_array_join() -> None:
    validate_ro_query(
        "SELECT * FROM my_table LEFT ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
        allowed_tables={"my_table"},
    )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, other_table LEFT ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
            allowed_tables={"my_table"},
        )
