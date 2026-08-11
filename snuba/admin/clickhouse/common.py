from __future__ import annotations

import re
from collections.abc import MutableMapping

from sql_metadata import Parser, QueryType  # type: ignore[import-untyped]

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import (
    DEFAULT_CLICKHOUSE_HTTP_PORT,
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
    connection_cache,
    use_clickhouse_connect_driver,
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


def is_valid_node(host: str, port: int, cluster: ClickhouseCluster, storage_name: str) -> bool:
    nodes = [
        cluster.get_query_node(),
    ]
    try:
        nodes.extend([*cluster.get_local_nodes(), *cluster.get_distributed_nodes()])
    except Exception as e:
        raise InvalidNodeError(
            f"Error getting nodes for storage {storage_name}",
            extra_data={
                "error": str(e),
                "host": host,
                "port": port,
                "nodes": ",".join([node.host_name for node in nodes]),
            },
        ) from e

    return any(node.host_name == host and node.native_port == port for node in nodes)


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
    storage_name: str,
) -> None:
    if not is_valid_node(clickhouse_host, clickhouse_port, cluster, storage_name):
        raise InvalidNodeError(
            f"host {clickhouse_host} and port {clickhouse_port} are not valid",
            extra_data={
                "host": clickhouse_host,
                "port": clickhouse_port,
                "query_host": cluster.get_query_node().host_name,
                "query_port": cluster.get_query_node().native_port,
            },
        )


NODE_CONNECTIONS: MutableMapping[str, ClickhousePool] = {}


def _build_validated_pool(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    cluster: ClickhouseCluster,
    database: str,
    username: str,
    password: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    # Single chokepoint for admin ClickhousePool acquisition. A pool ships the
    # user/password to the node (the native protocol's first hello packet, or
    # the HTTP auth header), so an unvalidated host means credentials reach
    # whatever listener answers. All admin helpers must go through here — never
    # acquire a pool from the connection cache directly in this module. The
    # regression test test_no_direct_clickhouse_pool_construction_in_admin
    # enforces this.
    _validate_node(clickhouse_host, clickhouse_port, cluster, storage_name)
    # Go through the shared connection cache so the driver (native vs
    # clickhouse-connect/HTTP) is selected by the use_clickhouse_connect_driver
    # sentry-option, behind the abstract ClickhousePool type, just like the
    # cluster's own connections.
    #
    # Pick the HTTP port for the clickhouse-connect (HTTP) driver. (The native
    # driver ignores http_port and talks to clickhouse_port directly, so it is
    # unaffected either way.)
    #
    # cluster.get_http_port() is the port of the cluster's configured query
    # endpoint, which may be a load balancer / proxy on a non-default port. It
    # is correct *only* when we are connecting to that endpoint — i.e. the query
    # node, the same host the normal read path reaches on
    # cluster.get_http_port() (this is what get_ro_query_node_connection, and
    # thus the tracing/querylog/cardinality tools, rely on). For any other host
    # — a specific individual node selected by host in the admin tools — that
    # port does not apply: an individual node serves HTTP on the well-known
    # default port, so use that instead.
    query_node = cluster.get_query_node()
    is_query_node = (
        clickhouse_host == query_node.host_name and clickhouse_port == query_node.native_port
    )
    http_port = cluster.get_http_port() if is_query_node else DEFAULT_CLICKHOUSE_HTTP_PORT
    return connection_cache.get_node_connection(
        client_settings,
        ClickhouseNode(clickhouse_host, clickhouse_port, http_port=http_port),
        username,
        password,
        database,
        secure=False,
        ca_certs=None,
        verify=False,
    )


def _driver_cache_token() -> str:
    # Part of the admin connection cache keys so that flipping the
    # use_clickhouse_connect_driver runtime flag re-resolves admin connections
    # to the new driver, instead of returning a pool pinned to whichever driver
    # was active when the entry was first cached. This keeps admin traffic
    # switchable at runtime, like the cluster query/reader paths.
    return "connect" if use_clickhouse_connect_driver() else "native"


def _settings_cache_token(client_settings: ClickhouseClientSettings) -> str:
    # Part of the admin connection cache keys because the ClickHouse settings
    # (and, for the read-only getters, the credentials) a pool is built with are
    # baked in at construction. Without this, two tools asking for the same
    # storage/host with different profiles would collide: whichever ran first
    # would win and the other would silently execute against the wrong pool. For
    # example System Queries (QUERY -> readonly user, 25s cap) and the
    # Cardinality Analyzer (CARDINALITY_ANALYZER -> trace user, max_threads=10,
    # 60s cap) both reach generic_metrics_distributions on the query node.
    return client_settings.name


def get_ro_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    assert client_settings in {
        ClickhouseClientSettings.QUERY,
        ClickhouseClientSettings.QUERYLOG,
        ClickhouseClientSettings.TRACING,
        ClickhouseClientSettings.CARDINALITY_ANALYZER,
    }, (
        "admin can only use QUERY, QUERYLOG, TRACING or CARDINALITY_ANALYZER "
        "ClickhouseClientSettings"
    )

    storage = _get_storage(storage_name)

    key = f"{storage.get_storage_key()}-{clickhouse_host}-{_settings_cache_token(client_settings)}-{_driver_cache_token()}"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

    cluster = storage.get_cluster()
    database = cluster.get_database()

    if (
        client_settings == ClickhouseClientSettings.QUERY
        or client_settings == ClickhouseClientSettings.QUERYLOG
    ):
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
    NODE_CONNECTIONS[key] = connection
    return connection


CLUSTER_CONNECTIONS: MutableMapping[str, ClickhousePool] = {}


def get_ro_query_node_connection(
    storage_name: str, client_settings: ClickhouseClientSettings
) -> ClickhousePool:
    key = f"{storage_name}-{_settings_cache_token(client_settings)}-{_driver_cache_token()}"
    if key in CLUSTER_CONNECTIONS:
        return CLUSTER_CONNECTIONS[key]

    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    connection_id = cluster.get_connection_id()
    connection = get_ro_node_connection(
        connection_id.hostname, connection_id.tcp_port, storage_name, client_settings
    )

    CLUSTER_CONNECTIONS[key] = connection
    return connection


def get_sudo_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    storage = _get_storage(storage_name)

    key = f"{storage.get_storage_key()}-{clickhouse_host}-sudo-{_settings_cache_token(client_settings)}-{_driver_cache_token()}"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

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
    NODE_CONNECTIONS[key] = connection
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

    key = f"{storage.get_storage_key()}-{clickhouse_host}-clusterless-{database}-{_settings_cache_token(client_settings)}-{_driver_cache_token()}"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

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
    NODE_CONNECTIONS[key] = connection
    return connection


def get_ro_clusterless_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    assert client_settings in {
        ClickhouseClientSettings.QUERY,
        ClickhouseClientSettings.QUERYLOG,
    }, "ro clusterless connections must use a read-only client settings profile"

    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database = cluster.get_database()

    key = f"{storage.get_storage_key()}-{clickhouse_host}-clusterless-ro-{database}-{_settings_cache_token(client_settings)}-{_driver_cache_token()}"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

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
    NODE_CONNECTIONS[key] = connection
    return connection


def _end_of_sql_string_literal(sql_query: str, start: int) -> int | None:
    """Return the index just past a string literal that starts at ``start``.

    ``start`` must point at the opening ``'`` or ``"``. Walks forward handling:
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


# sql_metadata does not report table functions in Parser.tables, so a query
# sourced only from one reaches the allowed_tables check with an empty table set
# and passes it. ARRAY JOIN is excluded: it takes an expression list, not a table.
_TABLE_POSITION_CALL_RE = re.compile(r"(?<!array )\b(?:from|join)\s+(\w+)\s*\(")

# Fanning a read out across replicas is normal in these tools, so cluster and
# clusterAllReplicas stay allowed -- as they already are in the system-queries
# validator. Note this leaves allowed_tables reachable through them.
_ALLOWED_TABLE_FUNCTIONS = frozenset({"cluster", "clusterallreplicas"})

# Table functions that can read data allowed_tables never authorized: off the
# node (network, filesystem) or across local tables. Matched anywhere rather
# than only in a table position, since a FROM list can reach them past a comma
# -- `FROM allowed, merge('default', '.*')` -- or from inside a subquery. None
# collide with a scalar function name; `format` and `values` are left out
# because they do.
_DISALLOWED_TABLE_FUNCTIONS = (
    "azureBlobStorage",
    "azureBlobStorageCluster",
    "deltaLake",
    "dictionary",
    "executable",
    "file",
    "fileCluster",
    "gcs",
    "hdfs",
    "hdfsCluster",
    "hudi",
    "iceberg",
    "icebergS3",
    "jdbc",
    "loop",
    "merge",
    "mergeTreeIndex",
    "mongodb",
    "mysql",
    "odbc",
    "postgresql",
    "redis",
    "remote",
    "remoteSecure",
    "s3",
    "s3Cluster",
    "sqlite",
    "url",
    "urlCluster",
    "view",
    "viewIfPermitted",
)
_DISALLOWED_TABLE_FUNCTION_RE = re.compile(
    r"\b(?:" + "|".join(fn.lower() for fn in _DISALLOWED_TABLE_FUNCTIONS) + r")\s*\("
)


def _reject_table_functions(normalized_query: str) -> None:
    """Reject table functions. Query must be lower cased, literal stripped and
    whitespace collapsed for the patterns above to match reliably."""
    disallowed = _DISALLOWED_TABLE_FUNCTION_RE.search(normalized_query)
    if disallowed:
        raise InvalidCustomQuery(
            f"table function {disallowed.group().rstrip('( ')} is not allowed in the query"
        )

    # Backstop for table functions not named above (synthetic generators, and
    # anything ClickHouse adds later) sitting where a table belongs.
    for match in _TABLE_POSITION_CALL_RE.finditer(normalized_query):
        name = match.group(1)
        if name not in _ALLOWED_TABLE_FUNCTIONS:
            raise InvalidCustomQuery(f"table function {name} is not allowed in the query")


def validate_ro_query(sql_query: str, allowed_tables: set[str] | None = None) -> None:
    """
    Validates that the query is a safe read-only query.

    If allowed_tables is provided, ensures the 'from' clause contains
    an allowed table. All tables are allowed otherwise.

    Raises InvalidCustomQuery if query is invalid or not allowed.
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

    # Must run before the allowed_tables check below, which cannot see them.
    _reject_table_functions(" ".join(lowered.split()))

    parsed = Parser(sql_query.lower())

    if parsed.query_type != QueryType.SELECT:
        raise InvalidCustomQuery("Only SELECT queries are allowed")

    # This parser doesn't handle ARRAY JOIN clauses correctly, so do some
    # massaging to get around that. What ends up happening is that the columns
    # in the ARRAY JOIN are treated as table aliases, so end up in this dictionary
    # as well as in the tables list. E.g. FROM x ARRAY JOIN y AS z becomes
    # tables_aliases = {'ARRAY': x, 'z': y} and tables = ['x', 'y'].
    # Confusingly it will also sometimes lower case ARRAY, so check for both.
    tables_set = set(parsed.tables)
    array_join = None
    array_join_keys = ["ARRAY", "array", "LEFT", "left"]
    for ak in array_join_keys:
        if ak in parsed.tables_aliases:
            array_join = ak
            break

    if array_join:
        for v in parsed.tables_aliases.values():
            tables_set.discard(v)  # Remove the columns

        tables_set.add(parsed.tables_aliases[array_join])  # Add the table back

    if allowed_tables and not tables_set.issubset(allowed_tables):
        raise InvalidCustomQuery(
            f"Invalid FROM clause, only the following tables are allowed: {allowed_tables}"
        )


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
