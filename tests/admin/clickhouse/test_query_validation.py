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


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM url('http://169.254.169.254/latest/meta-data/', CSV, 'a String')",
        "SELECT * FROM remote('other-host:9000', system.users)",
        "SELECT * FROM mysql('host:3306', 'db', 'table', 'user', 'password')",
        "SELECT * FROM s3('http://host/key', 'CSV', 'a String')",
        "SELECT * FROM merge('default', '.*')",
        "SELECT * FROM numbers(10)",
        # Not sitting directly after FROM.
        "SELECT * FROM my_table, url('http://evil/x', CSV, 'a String')",
        "SELECT * FROM my_table, merge('default', '.*')",
        "SELECT * FROM my_table, dictionary('d')",
        "SELECT * FROM my_table, view(SELECT * FROM system.users)",
        "SELECT * FROM my_table WHERE x IN (SELECT * FROM merge('default', '.*'))",
        "SELECT * FROM my_table WHERE x IN (SELECT * FROM remote('h:9000', system.users))",
        "SELECT * FROM\n  URL('http://evil/x', CSV, 'a String')",
        # Following an allowed one must not end the scan.
        "SELECT * FROM clusterAllReplicas('c', my_table) JOIN merge('default', '.*') USING x",
    ],
)
def test_table_functions_rejected(query: str) -> None:
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(query, allowed_tables={"my_table"})
    # Rejected for the unscoped tools (tracing) too, not just the scoped ones.
    with pytest.raises(InvalidCustomQuery):
        validate_ro_query(query)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM my_table ARRAY JOIN arrayMap(x -> x + 1, nums) AS n",
        "SELECT * FROM my_table LEFT ARRAY JOIN arrayZip(a, b) AS z",
        "SELECT * FROM (SELECT * FROM my_table) sub",
        "SELECT count() FROM my_table WHERE referrer IN ('a', 'b')",
        "SELECT * FROM my_table WHERE referrer = 'url(http://x)'",
        # Fanning out across replicas stays available to the admin tools.
        "SELECT * FROM clusterAllReplicas('c', my_table)",
        "SELECT * FROM cluster('c', my_table)",
    ],
)
def test_legitimate_queries_still_allowed(query: str) -> None:
    validate_ro_query(query, allowed_tables={"my_table"})
