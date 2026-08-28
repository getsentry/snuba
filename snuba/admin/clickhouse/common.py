from __future__ import annotations

import re
from collections.abc import Callable, Sequence

from sql_metadata import Parser, QueryType

from snuba import settings
from snuba.clickhouse.pool import ClickhousePool
from snuba.clusters.cluster import (
    DEFAULT_CLICKHOUSE_HTTP_PORT,
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
    build_pool,
)
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.serializable_exception import SerializableException


class InvalidNodeError(SerializableException):
    pass


class InvalidCustomQuery(SerializableException):
    pass


class InvalidStorageError(SerializableException):
    pass


def _node_connect_port(node: ClickhouseNode, cluster: ClickhouseCluster) -> int:
    if node.host_name == cluster.get_query_node().host_name:
        return cluster.get_port()
    return node.port if node.port is not None else DEFAULT_CLICKHOUSE_HTTP_PORT


def is_valid_node(
    host: str,
    port: int,
    cluster: ClickhouseCluster,
    storage_name: str | None = None,
    known_nodes: Sequence[ClickhouseNode] | None = None,
) -> bool:
    """Return whether host/port is a known node on the cluster.

    The configured query endpoint is always accepted from static cluster
    config. Topology discovery (local/distributed nodes) can fail independently
    of that endpoint being reachable, so discovery errors must not reject the
    query endpoint itself. For other hosts we still require a successful match
    against discovered topology.

    When `known_nodes` is provided (for example after `_get_cluster_state` has
    already discovered topology), validate against that snapshot instead of
    re-querying ClickHouse. A second discovery can fail transiently and mark a
    just-discovered node as invalid.
    """
    query_node = cluster.get_query_node()
    if host == query_node.host_name and port == _node_connect_port(query_node, cluster):
        return True

    if known_nodes is not None:
        return any(
            node.host_name == host and _node_connect_port(node, cluster) == port
            for node in known_nodes
        )

    nodes: list[ClickhouseNode] = [query_node]
    topology_error: Exception | None = None
    try:
        nodes.extend(list(cluster.get_local_nodes()))
    except Exception as e:
        topology_error = e
    try:
        nodes.extend(list(cluster.get_distributed_nodes()))
    except Exception as e:
        topology_error = e if topology_error is None else topology_error

    if any(node.host_name == host and _node_connect_port(node, cluster) == port for node in nodes):
        return True

    if topology_error is not None:
        subject = f"storage {storage_name}" if storage_name is not None else "cluster"
        raise InvalidNodeError(
            f"Error getting nodes for {subject}",
            extra_data={
                "error": str(topology_error),
                "host": host,
                "port": port,
                "nodes": ",".join([node.host_name for node in nodes]),
            },
        ) from topology_error

    return False


def _get_storage(storage_name: str) -> ReadableTableStorage:
    storage_key = None
    try:
        storage_key = StorageKey(storage_name)
    except ValueError as e:
        raise InvalidStorageError(
            f"storage {storage_name} is not a valid storage name",
            extra_data={"storage_name": storage_name},
        ) from e
    return get_storage(storage_key)


def _validate_node(
    clickhouse_host: str,
    clickhouse_port: int,
    cluster: ClickhouseCluster,
    storage_name: str | None = None,
    known_nodes: Sequence[ClickhouseNode] | None = None,
) -> None:
    if not is_valid_node(
        clickhouse_host, clickhouse_port, cluster, storage_name, known_nodes=known_nodes
    ):
        raise InvalidNodeError(
            f"host {clickhouse_host} and port {clickhouse_port} are not valid",
            extra_data={
                "host": clickhouse_host,
                "port": clickhouse_port,
                "query_host": cluster.get_query_node().host_name,
                "query_port": cluster.get_port(),
            },
        )


def _build_validated_pool(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str | None,
    cluster: ClickhouseCluster,
    database: str,
    username: str,
    password: str,
    client_settings: ClickhouseClientSettings,
    known_nodes: Sequence[ClickhouseNode] | None = None,
    validate_node: bool = True,
) -> ClickhousePool:
    # Single chokepoint for admin ClickhousePool acquisition. A pool ships the
    # user/password to the node (HTTP auth header), so an unvalidated host means
    # credentials reach whatever listener answers. All admin helpers must go
    # through here. The regression test
    # test_no_direct_clickhouse_pool_construction_in_admin enforces this.
    #
    # `validate_node=False` is only for copy-tables CREATE against a host that
    # is not (yet) in the source cluster topology. That path is sudo-gated at
    # the view layer. Every other helper must leave the default on.
    if validate_node:
        _validate_node(
            clickhouse_host, clickhouse_port, cluster, storage_name, known_nodes=known_nodes
        )
        # Query-endpoint traffic uses the cluster Envoy listen port. Replica
        # (by-host) traffic uses 8123 on that node.
        query_node = cluster.get_query_node()
        envoy_port = cluster.get_port()
        is_query_endpoint = (
            clickhouse_host == query_node.host_name and clickhouse_port == envoy_port
        )
        connect_port = envoy_port if is_query_endpoint else DEFAULT_CLICKHOUSE_HTTP_PORT
    else:
        connect_port = clickhouse_port
    return build_pool(
        client_settings,
        ClickhouseNode(clickhouse_host, connect_port),
        username,
        password,
        database,
        secure=cluster.get_secure(),
        ca_certs=cluster.get_ca_certs(),
        verify=cluster.get_verify(),
    )


def get_ro_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    # Compare by name: tests that importlib.reload(cluster) leave stale Enum
    # identities in other modules, so `in {EnumMember, ...}` can false-negative.
    allowed = {
        ClickhouseClientSettings.QUERY.name,
        ClickhouseClientSettings.QUERYLOG.name,
        ClickhouseClientSettings.TRACING.name,
        ClickhouseClientSettings.CARDINALITY_ANALYZER.name,
    }
    assert getattr(client_settings, "name", None) in allowed, (
        "admin can only use QUERY, QUERYLOG, TRACING or CARDINALITY_ANALYZER "
        "ClickhouseClientSettings"
    )

    storage = _get_storage(storage_name)

    cluster = storage.get_cluster()
    database = cluster.get_database()

    if getattr(client_settings, "name", None) in {
        ClickhouseClientSettings.QUERY.name,
        ClickhouseClientSettings.QUERYLOG.name,
    }:
        username = settings.CLICKHOUSE_READONLY_USER
        password = settings.CLICKHOUSE_READONLY_PASSWORD
    else:
        username = settings.CLICKHOUSE_TRACE_USER
        password = settings.CLICKHOUSE_TRACE_PASSWORD

    connection = _build_validated_pool(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        cluster,
        database,
        username,
        password,
        client_settings,
    )
    return connection


def get_ro_query_node_connection(
    storage_name: str, client_settings: ClickhouseClientSettings
) -> ClickhousePool:
    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    connection_id = cluster.get_connection_id()
    connection = get_ro_node_connection(
        connection_id.hostname, cluster.get_port(), storage_name, client_settings
    )

    return connection


def get_ro_cluster_node_connection(
    cluster: ClickhouseCluster,
    node: ClickhouseNode,
    client_settings: ClickhouseClientSettings,
    known_nodes: Sequence[ClickhouseNode] | None = None,
) -> ClickhousePool:
    """Read-only connection to a known node on a configured cluster.

    Unlike the storage-keyed helpers, this path works for clusters with no
    registered storage. It still goes through `_build_validated_pool`, so the
    host/port must belong to the cluster and only the global readonly
    credentials are used.

    Pass `known_nodes` when the caller already discovered topology for this
    request so validation does not re-query and risk a transient false reject.
    """
    allowed = {
        ClickhouseClientSettings.QUERY.name,
        ClickhouseClientSettings.QUERYLOG.name,
        ClickhouseClientSettings.TRACING.name,
        ClickhouseClientSettings.CARDINALITY_ANALYZER.name,
    }
    assert getattr(client_settings, "name", None) in allowed, (
        "admin can only use QUERY, QUERYLOG, TRACING or CARDINALITY_ANALYZER "
        "ClickhouseClientSettings"
    )

    if getattr(client_settings, "name", None) in {
        ClickhouseClientSettings.QUERY.name,
        ClickhouseClientSettings.QUERYLOG.name,
    }:
        username = settings.CLICKHOUSE_READONLY_USER
        password = settings.CLICKHOUSE_READONLY_PASSWORD
    else:
        username = settings.CLICKHOUSE_TRACE_USER
        password = settings.CLICKHOUSE_TRACE_PASSWORD

    return _build_validated_pool(
        node.host_name,
        _node_connect_port(node, cluster),
        None,
        cluster,
        cluster.get_database(),
        username,
        password,
        client_settings,
        known_nodes=known_nodes,
    )


def get_sudo_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    storage = _get_storage(storage_name)

    cluster = storage.get_cluster()
    database = cluster.get_database()
    (clickhouse_user, clickhouse_password) = cluster.get_credentials()

    connection = _build_validated_pool(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        cluster,
        database,
        clickhouse_user,
        clickhouse_password,
        client_settings,
    )
    return connection


def get_clusterless_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database = cluster.get_database()

    (clickhouse_user, clickhouse_password) = cluster.get_credentials()
    connection = _build_validated_pool(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        cluster,
        database,
        clickhouse_user,
        clickhouse_password,
        client_settings,
    )
    return connection


def get_unvalidated_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    """Connect using this storage's credentials without cluster membership checks.

    Copy-tables uses this for the CREATE target so schemas can be applied on a
    host that is not a member of the source cluster (for example a new ARM
    cluster). The host is not checked against topology; credentials still come
    from the source storage's cluster. Callers must already be sudo-gated.
    """
    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database = cluster.get_database()
    (clickhouse_user, clickhouse_password) = cluster.get_credentials()
    return _build_validated_pool(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        cluster,
        database,
        clickhouse_user,
        clickhouse_password,
        client_settings,
        validate_node=False,
    )


def get_ro_clusterless_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    # Compare by name: same reload-safe rule as get_ro_node_connection.
    allowed = {
        ClickhouseClientSettings.QUERY.name,
        ClickhouseClientSettings.QUERYLOG.name,
    }
    assert getattr(client_settings, "name", None) in allowed, (
        "ro clusterless connections must use a read-only client settings profile"
    )

    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database = cluster.get_database()

    connection = _build_validated_pool(
        clickhouse_host,
        clickhouse_port,
        storage_name,
        cluster,
        database,
        settings.CLICKHOUSE_READONLY_USER,
        settings.CLICKHOUSE_READONLY_PASSWORD,
        client_settings,
    )
    return connection


def _end_of_sql_string_literal(sql_query: str, start: int) -> int | None:
    """Return the index just past a string literal that starts at ``start``.

    ``start`` must point at the opening ``'``, ``"``, or backtick. Walks forward handling:
    - backslash escapes (``\'``, ``\"``)
    - SQL-style doubled quotes (``''``, ``""``)

    Returns ``None`` when no matching closer is found.
    """
    quote = sql_query[start]
    i = start + 1
    n = len(sql_query)
    while i < n:
        ch = sql_query[i]
        if ch == "\\" and i + 1 < n:
            # Skip the escaped character (e.g. \' keeps the quote inside).
            i += 2
            continue
        if ch == quote:
            # Doubled quote is an escaped quote, not a terminator.
            if i + 1 < n and sql_query[i + 1] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    return None


def _sql_quotes_are_balanced(sql_query: str) -> bool:
    """Return True when single/double quotes are balanced, honoring escapes.

    Understands backslash escapes (``\'``) and SQL-style doubled quotes (``''``),
    so values like ``O'Brien`` escaped as ``O\'Brien`` do not look unbalanced.
    """
    i = 0
    n = len(sql_query)
    while i < n:
        ch = sql_query[i]
        if ch == "\\":
            # Outside a string, skip an escaped character if present.
            i += 2 if i + 1 < n else 1
            continue
        if ch in ("'", '"'):
            end = _end_of_sql_string_literal(sql_query, i)
            if end is None:
                return False
            i = end
            continue
        i += 1
    return True


def _strip_sql_string_literals(sql_query: str) -> str:
    """Replace quoted string contents with empty quotes for safety checks.

    Lets validators ignore disallowed tokens that only appear inside literals
    (e.g. a referrer filter value containing ``--`` or ``delete``).
    """
    out: list[str] = []
    i = 0
    n = len(sql_query)
    while i < n:
        ch = sql_query[i]
        if ch in ("'", '"'):
            end = _end_of_sql_string_literal(sql_query, i)
            if end is None:
                # Unbalanced quote; leave remainder as-is for the caller to reject.
                out.append(sql_query[i:])
                break
            # Keep the delimiters so surrounding SQL shape is preserved, drop contents.
            out.append(ch)
            out.append(ch)
            i = end
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _field_after(line: str, key: str) -> str | None:
    marker = f"{key}:"
    idx = line.lower().find(marker)
    if idx == -1:
        return None
    rest = line[idx + len(marker) :].lstrip()
    for sep in (",", " ", "\t"):
        rest = rest.split(sep, 1)[0]
    return rest or None


def _tables_from_query_tree(explain_output: str) -> set[str]:
    """Tables ClickHouse reports for a SELECT, excluding ARRAY JOIN columns."""
    tables: set[str] = set()
    for raw_line in explain_output.splitlines():
        line = raw_line.strip()
        if line.startswith("TABLE_FUNCTION") or _field_after(line, "table_function_name"):
            raise InvalidCustomQuery("table functions are not allowed in the query")
        name = _field_after(line, "table_name")
        if name is None:
            continue
        tables.add(name.rsplit(".", 1)[-1])
    return tables


def _tables_from_explain(sql_query: str, connection: ClickhousePool) -> set[str]:
    sql = sql_query.strip().rstrip(";")
    result = connection.execute_explain(f"EXPLAIN QUERY TREE {sql}")
    text = "\n".join(str(row[0]) for row in result.results if row)
    return _tables_from_query_tree(text)


def validate_ro_query(
    sql_query: str,
    allowed_tables: set[str] | None = None,
    connection: ClickhousePool | None = None,
    get_connection: Callable[[], ClickhousePool] | None = None,
) -> ClickhousePool | None:
    """
    Validates that the query is a safe read-only query.

    Keyword and SELECT checks always run first. get_connection is only called
    after those pass, so ALTER/INSERT never opens ClickHouse. When a pool is
    available, table names come from EXPLAIN QUERY TREE. Without one,
    sql_metadata is used and ARRAY JOIN columns stay in the set (fail closed).

    Returns the pool if one was provided or created.
    """
    if not _sql_quotes_are_balanced(sql_query):
        raise InvalidCustomQuery("Unbalanced quotes detected in query")

    # Ignore tokens that only appear inside string literals when scanning for
    # disallowed keywords/comments. Parsing still uses the original SQL.
    lowered = _strip_sql_string_literals(sql_query).lower()
    # Enhanced disallowed keywords to prevent SQL injection and data modification
    disallowed_keywords = [
        "insert",
        "update",
        "delete",
        "drop",
        "create",
        "alter",
        "truncate",
        "replace",
        ";",  # Prevent query chaining
        "--",  # Prevent comment-based injection
        "/*",  # Prevent multi-line comment injection
        "*/",
        "exec",
        "execute",
        "xp_",  # Prevent stored procedure execution
    ]

    for kw in disallowed_keywords:
        if kw == "replace":
            if re.search(r"\breplace\b", lowered):
                raise InvalidCustomQuery(f"{kw} is not allowed in the query")
        elif kw in lowered:
            raise InvalidCustomQuery(f"{kw} is not allowed in the query")

    parsed = Parser(sql_query.lower())

    if parsed.query_type != QueryType.SELECT:
        raise InvalidCustomQuery("Only SELECT queries are allowed")

    pool = connection if connection is not None else (get_connection() if get_connection else None)
    if pool is not None:
        tables_set = _tables_from_explain(sql_query, pool)
    elif allowed_tables:
        # sql_metadata reports ARRAY JOIN columns as tables. Without ClickHouse
        # to resolve them, leave those names in the set so the allowlist fails
        # closed instead of guessing which dotted names are columns.
        tables_set = set(parsed.tables)
    else:
        return None

    if allowed_tables and (not tables_set or not tables_set.issubset(allowed_tables)):
        raise InvalidCustomQuery(
            f"Invalid FROM clause, only the following tables are allowed: {allowed_tables}"
        )
    return pool


def format_predefined_sql(sql: str) -> str:
    """Strip the 4-space class-body indent from predefined SQL multiline strings.

    Predefined query SQL is authored inside class bodies, so every line carries at
    least four leading spaces from Python indentation. Callers should send the
    result of this helper to the frontend so the editor does not need to guess
    how much to strip. Indentation beyond those four spaces is preserved.
    """
    return "\n".join(
        line[4:] if line.startswith("    ") else line for line in sql.split("\n")
    ).strip()


class PreDefinedQuery:
    sql: str

    @classmethod
    def to_json(cls) -> dict[str, str]:
        return {
            "sql": cls.sql,
            "description": cls.__doc__ or "",
            "name": cls.__name__,
        }
