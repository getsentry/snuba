import re

from clickhouse_driver.errors import ErrorCodes

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog
from snuba.admin.auth_roles import ExecuteSudoSystemQuery
from snuba.admin.clickhouse.common import (
    InvalidCustomQuery,
    get_clusterless_node_connection,
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
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    sql: str,
    sudo: bool,
    clusterless_mode: bool,
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

    if clusterless_mode:
        connection = get_clusterless_node_connection(
            clickhouse_host, clickhouse_port, storage_name, settings
        )
        return connection.execute(query=sql, with_column_types=True)

    connection = (
        get_ro_node_connection(clickhouse_host, clickhouse_port, storage_name, settings)
        if not sudo
        else get_sudo_node_connection(clickhouse_host, clickhouse_port, storage_name, settings)
    )
    query_result = connection.execute(query=sql, with_column_types=True)
    return query_result


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

KILL_COMMAND_RE = re.compile(
    r"""
        ^
        (KILL\sMUTATION\sWHERE)
        \s
        .*\s*=\s*.*
        ;?
        $
    """,
    re.IGNORECASE + re.VERBOSE,
)

SYSTEM_COMMAND_RE = re.compile(
    r"""
        ^
        (SYSTEM)
        \s
        (?!SHUTDOWN\b)(?!KILL\b)
        [\w\s'\-_]+
        ;? # Optional semicolon
        $
    """,
    re.IGNORECASE + re.VERBOSE,
)

SYSTEM_DROP_COMMAND_RE = re.compile(
    r"""
        ^
        (SYSTEM\s+DROP\s+REPLICA)
        [\w\s,=()*+<>'%\-\/:\.`]+
        ;? # Optional semicolon
        $
    #""",
    re.IGNORECASE + re.VERBOSE,
)

OPTIMIZE_QUERY_RE = re.compile(
    r"""^
        (OPTIMIZE\sTABLE)
        \s
        [\w\s_\-']+
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


def is_query_using_only_system_tables(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    sql_query: str,
    clusterless_mode: bool,
) -> bool:
    """
    Run the EXPLAIN QUERY TREE on the given sql_query and check that the only tables
    in the query are system tables.
    """
    sql_query = sql_query.strip().rstrip(";") if sql_query.endswith(";") else sql_query
    explain_query_tree_query = (
        f"EXPLAIN QUERY TREE {sql_query} SETTINGS allow_experimental_analyzer = 1"
    )
    explain_query_tree_result = _run_sql_query_on_host(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        explain_query_tree_query,
        False,
        clusterless_mode,
    )

    for line in explain_query_tree_result.results:
        line = line[0].strip()
        # We don't allow table functions (except clusterAllReplicas/merge) for now as the clickhouse analyzer isn't good enough yet to resolve those tables
        if (
            line.startswith("TABLE_FUNCTION")
            and "table_function_name: clusterAllReplicas" not in line
            and "table_function_name: merge" not in line
        ):
            return False
        if line.startswith("TABLE"):
            match = re.search(r"table_name:\s*(\S+)", line, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                if not table_name.startswith("system."):
                    return False

    return True


def is_valid_system_query(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    sql_query: str,
    clusterless_mode: bool,
) -> bool:
    """
    Validation based on Query Tree and AST to ensure the query is a valid select query.
    """
    explain_ast_query = f"EXPLAIN AST {sql_query}"
    disallowed_ast_nodes = ["AlterQuery", "AlterCommand", "DropQuery", "InsertQuery"]
    explain_ast_result = _run_sql_query_on_host(
        clickhouse_host, clickhouse_port, storage_name, explain_ast_query, False, clusterless_mode
    )

    for node in disallowed_ast_nodes:
        if any(line[0].lstrip().startswith(node) for line in explain_ast_result.results):
            return False

    return is_query_using_only_system_tables(
        clickhouse_host, clickhouse_port, storage_name, sql_query, clusterless_mode
    )


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
    matches = False
    to_match = [SYSTEM_COMMAND_RE, KILL_COMMAND_RE, SYSTEM_DROP_COMMAND_RE]
    for pattern in to_match:
        matches |= bool(pattern.match(sql_query))
    return matches


def is_query_optimize(sql_query: str) -> bool:
    """
    Validates whether we are running something like OPTIMIZE TABLE ...
    """
    sql_query = " ".join(sql_query.split())
    match = OPTIMIZE_QUERY_RE.match(sql_query)
    return True if match else False


def is_query_alter(sql_query: str) -> bool:
    """
    Validates whether we are running something like ALTER TABLE ...
    """
    sql_query = " ".join(sql_query.split())
    match = ALTER_QUERY_RE.match(sql_query)
    return True if match else False


def validate_query(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    system_query_sql: str,
    sudo_mode: bool,
    clusterless_mode: bool,
) -> None:
    if is_query_describe(system_query_sql) or is_query_show(system_query_sql):
        return

    if sudo_mode and (
        is_system_command(system_query_sql)
        or is_query_alter(system_query_sql)
        or is_query_optimize(system_query_sql)
    ):
        return

    if is_valid_system_query(
        clickhouse_host, clickhouse_port, storage_name, system_query_sql, clusterless_mode
    ):
        if sudo_mode:
            raise InvalidCustomQuery("Query is valid but sudo is not allowed")
        return

    raise InvalidCustomQuery("Query is invalid")


def run_system_query_on_host_with_sql(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    system_query_sql: str,
    sudo_mode: bool,
    clusterless_mode: bool,
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

    validate_query(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        system_query_sql,
        sudo_mode,
        clusterless_mode,
    )

    try:
        return _run_sql_query_on_host(
            clickhouse_host,
            clickhouse_port,
            storage_name,
            system_query_sql,
            sudo_mode,
            clusterless_mode,
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
                    "clusterless_mode": clusterless_mode,
                },
                notify=sudo_mode,
            )
        if clusterless_mode and not sudo_mode:
            audit_log.record(
                user.email,
                AuditLogAction.RAN_CLUSTERLESS_SYSTEM_QUERY,
                {
                    "query": system_query_sql,
                    "clickhouse_port": clickhouse_port,
                    "clickhouse_host": clickhouse_host,
                    "storage_name": storage_name,
                    "sudo_mode": sudo_mode,
                    "clusterless_mode": clusterless_mode,
                },
                notify=clusterless_mode,
            )
