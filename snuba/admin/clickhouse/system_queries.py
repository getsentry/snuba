import re

from clickhouse_driver.errors import ErrorCodes

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog
from snuba.admin.auth_roles import ExecuteSudoSystemQuery
from snuba.admin.clickhouse.common import (
    InvalidCustomQuery,
    get_ro_node_connection,
    get_sudo_node_connection,
)
from snuba.admin.user import AdminUser
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.utils.serializable_exception import SerializableException

audit_log = AuditLog()


class UnauthorizedForSudo(SerializableException):
    pass


def _run_sql_query_on_host(
    clickhouse_host: str, clickhouse_port: int, storage_name: str, sql: str, sudo: bool
) -> ClickhouseResult:
    """
    Run the SQL query. It should be validated before getting to this point
    """
    if storage_name == "querylog":
        # querylog readonly user profile has readonly=2 set, but if you try
        # and set readonly=2 as part of the request this will error since
        # clickhouse doesn't let you set readonly setting if readonly=2 in
        # the current settings https://github.com/ClickHouse/ClickHouse/blob/20.7/src/Access/SettingsConstraints.cpp#L243-L249
        settings = ClickhouseClientSettings.QUERYLOG
    else:
        settings = ClickhouseClientSettings.QUERY

    connection = (
        get_ro_node_connection(clickhouse_host, clickhouse_port, storage_name, settings)
        if not sudo
        else get_sudo_node_connection(
            clickhouse_host, clickhouse_port, storage_name, settings
        )
    )
    query_result = connection.execute(query=sql, with_column_types=True)
    return query_result


SYSTEM_QUERY_RE = re.compile(
    r"""
        ^ # Start
        (SELECT|select)
        \s
        (?P<select_statement>[\w\s\',()*+\-\/:]+|\*)
        \s
        (FROM|from)
        \s
        system.[a-z_]+
        (?P<extra>\s[\w\s,=()*+<>'%"\-\/:]+)?
        ;? # Optional semicolon
        $ # End
    """,
    re.VERBOSE,
)

DESCRIBE_QUERY_RE = re.compile(
    r"""
        ^ # Start
        (DESC|DESCRIBE)
        \s
        (TABLE)
        \s
        (?P<table_name>[\w\s]+|\*)
        ;? # Optional semicolon
        $ # End
    """,
    re.VERBOSE,
)

SHOW_QUERY_RE = re.compile(
    r"""
        ^ # Start
        (SHOW|show)
        \s
        [\w\s]+
        ;? # Optional semicolon
        $ # End
    """,
    re.VERBOSE,
)

SYSTEM_COMMAND_RE = re.compile(
    r"""
        ^
        (SYSTEM)
        \s
        (?!SHUTDOWN\b)(?!KILL\b)
        [\w\s]+
        ;? # Optional semicolon
        $
    """,
    re.IGNORECASE + re.VERBOSE,
)

ALTER_QUERY_RE = re.compile(
    r"""
        ^
        (ALTER|CREATE)
        \s
        [\w\s,=()*+<>'%"\-\/:\.`]+
        ;? # Optional semicolon
        $
    """,
    re.IGNORECASE + re.VERBOSE,
)


def is_query_select(sql_query: str) -> bool:
    """
    Simple validation to ensure query is a select command
    """
    sql_query = " ".join(sql_query.split())
    match = SYSTEM_QUERY_RE.match(sql_query)
    return True if match else False


def is_query_show(sql_query: str) -> bool:
    """
    Simple validation to ensure query is a show command
    """
    sql_query = " ".join(sql_query.split())
    match = SHOW_QUERY_RE.match(sql_query)
    return True if match else False


def is_query_describe(sql_query: str) -> bool:
    """
    Simple validation to ensure query is a describe command
    """
    sql_query = " ".join(sql_query.split())
    match = DESCRIBE_QUERY_RE.match(sql_query)
    return True if match else False


def is_system_command(sql_query: str) -> bool:
    """
    Validates whether we are running something like SYSTEM STOP MERGES
    """
    sql_query = " ".join(sql_query.split())
    match = SYSTEM_COMMAND_RE.match(sql_query)
    return True if match else False


def is_query_alter(sql_query: str) -> bool:
    """
    Validates whether we are running something like ALTER TABLE ...
    """
    sql_query = " ".join(sql_query.split())
    match = ALTER_QUERY_RE.match(sql_query)
    return True if match else False


def run_system_query_on_host_with_sql(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    system_query_sql: str,
    sudo_mode: bool,
    user: AdminUser,
) -> ClickhouseResult:
    if sudo_mode:
        can_sudo = any(
            isinstance(action, ExecuteSudoSystemQuery)
            for role in user.roles
            for action in role.actions
        )
        if not can_sudo:
            raise UnauthorizedForSudo()

    if is_query_select(system_query_sql):
        validate_system_query(system_query_sql)
    elif is_query_describe(system_query_sql):
        pass
    elif is_query_show(system_query_sql):
        pass
    elif sudo_mode and (
        is_system_command(system_query_sql) or is_query_alter(system_query_sql)
    ):
        pass
    else:
        raise InvalidCustomQuery("Query is invalid")

    try:
        return _run_sql_query_on_host(
            clickhouse_host, clickhouse_port, storage_name, system_query_sql, sudo_mode
        )
    except ClickhouseError as exc:
        # Don't send error to Snuba if it is an unknown table or column as it
        # will be too noisy
        if exc.code in (ErrorCodes.UNKNOWN_TABLE, ErrorCodes.UNKNOWN_IDENTIFIER):
            raise InvalidCustomQuery(f"Invalid query: {exc.message} {exc.code}")

        raise
    finally:
        if sudo_mode:
            audit_log.record(
                user.email,
                AuditLogAction.RAN_SUDO_SYSTEM_QUERY,
                {
                    "query": system_query_sql,
                    "clickhouse_port": clickhouse_port,
                    "clickhouse_host": clickhouse_host,
                    "storage_name": storage_name,
                    "sudo_mode": sudo_mode,
                },
                notify=sudo_mode,
            )


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
