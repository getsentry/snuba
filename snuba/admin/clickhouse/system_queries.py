import re

from clickhouse_driver.errors import ErrorCodes

from snuba.admin.clickhouse.common import InvalidCustomQuery, get_ro_node_connection
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.utils.serializable_exception import SerializableException


class NonExistentSystemQuery(SerializableException):
    pass


class InvalidResultError(SerializableException):
    pass


def _run_sql_query_on_host(
    clickhouse_host: str, clickhouse_port: int, storage_name: str, sql: str
) -> ClickhouseResult:
    """
    Run the SQL query. It should be validated before getting to this point
    """
    connection = get_ro_node_connection(
        clickhouse_host, clickhouse_port, storage_name, ClickhouseClientSettings.QUERY
    )
    query_result = connection.execute(query=sql, with_column_types=True)

    return query_result


SYSTEM_QUERY_RE = re.compile(
    r"""
        ^ # Start
        (SELECT|select)
        \s
        (?P<select_statement>[\w\s,()*+\-\/]+|\*)
        \s
        (FROM|from)
        \s
        system.[a-z_]+
        (?P<extra>\s[\w\s,=()*+<>'%"\-\/]+)?
        ;? # Optional semicolon
        $ # End
    """,
    re.VERBOSE,
)


def run_system_query_on_host_with_sql(
    clickhouse_host: str, clickhouse_port: int, storage_name: str, system_query_sql: str
) -> ClickhouseResult:
    validate_system_query(system_query_sql)
    try:
        return _run_sql_query_on_host(
            clickhouse_host, clickhouse_port, storage_name, system_query_sql
        )
    except ClickhouseError as exc:
        # Don't send error to Snuba if it is an unknown table or column as it
        # will be too noisy
        if exc.code in (ErrorCodes.UNKNOWN_TABLE, ErrorCodes.UNKNOWN_IDENTIFIER):
            raise InvalidCustomQuery(f"Invalid query: {exc.message} {exc.code}")

        raise


def validate_system_query(sql_query: str) -> None:
    """
    Simple validation to ensure query only attempts to access system tables and not
    any others. Will be replaced by AST parser eventually.

    Raises InvalidCustomQuery if query is invalid or not allowed.
    """
    sql_query = " ".join(sql_query.split())

    disallowed_keywords = ["select", "insert", "join"]

    match = SYSTEM_QUERY_RE.match(sql_query)

    if match is None:
        raise InvalidCustomQuery("Query is invalid")

    select_statement = match.group("select_statement")

    # Extremely quick and dirty way of ensuring there is not a nested select, insert or a join
    for kw in disallowed_keywords:
        if kw in select_statement.lower():
            raise InvalidCustomQuery(f"{kw} is not allowed here")

    extra = match.group("extra")

    # Unfortunately "extra" is pretty permissive right now, just ensure
    # there is no attempt to do a select, insert or join in there
    if extra is not None:
        for kw in disallowed_keywords:
            if kw in extra.lower():
                raise InvalidCustomQuery(f"{kw} is not allowed here")
