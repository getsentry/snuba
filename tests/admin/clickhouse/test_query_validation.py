from unittest.mock import Mock

import pytest

from snuba.admin.clickhouse.common import InvalidCustomQuery, validate_ro_query
from snuba.clickhouse.pool import ClickhouseResult


def _explain_connection(*tables: str) -> Mock:
    conn = Mock()
    text = "\n".join(f"TABLE id: 0, table_name: {table}" for table in tables)
    conn.execute_explain.return_value = ClickhouseResult(
        results=[(line,) for line in text.splitlines()]
    )
    return conn


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
        connection=_explain_connection("my_table", "other_table"),
    )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, other_table",
            allowed_tables={"my_table"},
            connection=_explain_connection("my_table", "other_table"),
        )


def test_allowed_tables_with_array_join() -> None:
    # ClickHouse EXPLAIN QUERY TREE reports the real table, not ARRAY JOIN columns.
    validate_ro_query(
        "SELECT * FROM my_table ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
        allowed_tables={"my_table"},
        connection=_explain_connection("my_table"),
    )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, other_table ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
            allowed_tables={"my_table"},
            connection=_explain_connection("my_table", "other_table"),
        )


def test_allowed_tables_with_left_array_join() -> None:
    validate_ro_query(
        "SELECT * FROM my_table LEFT ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
        allowed_tables={"my_table"},
        connection=_explain_connection("my_table"),
    )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, other_table LEFT ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value",
            allowed_tables={"my_table"},
            connection=_explain_connection("my_table", "other_table"),
        )


def test_table_aliased_as_array_does_not_drop_joined_tables() -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table AS array JOIN other_table",
            allowed_tables={"my_table"},
            connection=_explain_connection("my_table", "other_table"),
        )


def test_table_aliased_as_left_does_not_drop_joined_tables() -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table AS left JOIN other_table",
            allowed_tables={"my_table"},
            connection=_explain_connection("my_table", "other_table"),
        )


def test_array_join_does_not_drop_dotted_disallowed_table() -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table, default.secrets ARRAY JOIN tags.key AS k",
            allowed_tables={"my_table"},
            connection=_explain_connection("my_table", "secrets"),
        )
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM default.secrets ARRAY JOIN tags.key AS k",
            allowed_tables={"my_table"},
            connection=_explain_connection("secrets"),
        )


def test_backtick_array_join_alias_does_not_drop_dotted_table() -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM db.disallowed AS `array join`",
            allowed_tables={"my_table"},
            connection=_explain_connection("disallowed"),
        )


def test_explain_table_name_ignores_trailing_dump_fields() -> None:
    conn = Mock()
    conn.execute_explain.return_value = ClickhouseResult(
        results=[("TABLE id: 0, table_name: my_table, alias: t",)]
    )
    validate_ro_query(
        "SELECT * FROM my_table FINAL",
        allowed_tables={"my_table"},
        connection=conn,
    )


def test_array_join_without_explain_fails_closed() -> None:
    # Offline, sql_metadata still reports ARRAY JOIN columns as tables.
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(
            "SELECT * FROM my_table ARRAY JOIN tags.key AS tag_key",
            allowed_tables={"my_table"},
        )


def test_replace_functions_allowed() -> None:
    # ClickHouse replace* functions should be allowed in read-only queries
    validate_ro_query("SELECT replaceAll(message, 'foo', 'bar') FROM my_table")
    validate_ro_query("SELECT replaceRegexpAll(message, '[0-9]+', 'N') FROM my_table")
    validate_ro_query("SELECT replaceOne(message, 'x', 'y') FROM my_table")
    validate_ro_query("SELECT replaceRegexpOne(message, 'x', 'y') FROM my_table")


def test_replace_dml_rejected() -> None:
    # DML forms of REPLACE must still be blocked
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("REPLACE INTO my_table VALUES (1, 2, 3)")
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("REPLACE TABLE my_table SELECT * FROM other_table")


def test_disallowed_tokens_inside_string_literals_allowed() -> None:
    # Filter values may legitimately contain comment/keyword substrings.
    validate_ro_query("SELECT * FROM my_table WHERE referrer = 'api.org--test'")
    validate_ro_query("SELECT * FROM my_table WHERE msg = 'please delete me'")


def test_escaped_quotes_inside_literals_allowed() -> None:
    # Backslash-escaped and SQL-doubled quotes should not look unbalanced.
    validate_ro_query(r"SELECT * FROM my_table WHERE referrer = 'O\'Brien'")
    validate_ro_query("SELECT * FROM my_table WHERE referrer = 'O''Brien'")
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("SELECT * FROM my_table WHERE referrer = 'unterminated")


def test_comment_tokens_outside_literals_rejected() -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("SELECT * FROM my_table -- drop everything")
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query("SELECT * FROM my_table /* bad */")
