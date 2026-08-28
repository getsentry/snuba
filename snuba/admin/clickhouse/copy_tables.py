import re
from collections.abc import MutableMapping, Sequence
from dataclasses import dataclass
from typing import TypedDict, cast

from snuba.admin.clickhouse.common import (
    InvalidNodeError,
    _get_storage,
    _node_connect_port,
    get_clusterless_node_connection,
    is_valid_node,
)
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.pool import ClickhousePool
from snuba.clusters.cluster import (
    DEFAULT_CLICKHOUSE_HTTP_PORT,
    ClickhouseClientSettings,
    ClickhouseCluster,
)
from snuba.state.sentry_options import get_option
from snuba.utils.serializable_exception import SerializableException


class InvalidClusterName(SerializableException):
    pass


# Caller-supplied cluster names are interpolated into executed DDL and into
# clusterAllReplicas() on connections holding full cluster credentials.
CLUSTER_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

COPY_TABLES_ALLOWED_TARGET_HOSTS_OPTION = "admin.copy_tables_allowed_target_hosts"


def validate_cluster_name(cluster_name: str) -> str:
    if not CLUSTER_NAME_RE.match(cluster_name):
        raise InvalidClusterName(
            "cluster name must be 1-128 characters of letters, digits, underscores or dashes"
        )
    return cluster_name


def _split_host_port(raw: str) -> tuple[str, int | None]:
    value = raw.strip()
    if ":" in value:
        host, port_s = value.rsplit(":", 1)
        if port_s.isdigit():
            return host, int(port_s)
    return value, None


def parse_target_host(raw: str) -> tuple[str, int]:
    """Split ``host`` or ``host:port``; default port is 8123."""
    host, port = _split_host_port(raw)
    return host, port if port is not None else DEFAULT_CLICKHOUSE_HTTP_PORT


def target_host_is_allowlisted(host: str, port: int) -> bool:
    """Hostname-only entries match any port; host:port must match both."""
    host_key = host.lower()
    for entry in cast("list[str]", get_option(COPY_TABLES_ALLOWED_TARGET_HOSTS_OPTION, [])):
        entry_host, entry_port = _split_host_port(str(entry))
        if entry_host.lower() == host_key and (entry_port is None or entry_port == port):
            return True
    return False


def _is_cluster_node(host: str, port: int, cluster: ClickhouseCluster, storage_name: str) -> bool:
    try:
        if is_valid_node(host, port, cluster, storage_name):
            return True
        topology_port = _http_port_for_host(host, cluster)
        return topology_port != port and is_valid_node(host, topology_port, cluster, storage_name)
    except InvalidNodeError:
        return False


def assert_target_host_allowed(
    host: str,
    port: int,
    cluster: ClickhouseCluster,
    storage_name: str,
) -> None:
    if target_host_is_allowlisted(host, port) or _is_cluster_node(
        host, port, cluster, storage_name
    ):
        return
    raise ValueError(
        f"{host}:{port} is not a known cluster node and is not in "
        f"{COPY_TABLES_ALLOWED_TARGET_HOSTS_OPTION}"
    )


def _http_port_for_host(host: str, cluster: ClickhouseCluster) -> int:
    if host == cluster.get_query_node().host_name:
        return cluster.get_port()
    for node in (*cluster.get_local_nodes(), *cluster.get_distributed_nodes()):
        if node.host_name == host:
            return _node_connect_port(node, cluster)
    return DEFAULT_CLICKHOUSE_HTTP_PORT


@dataclass
class TableStatement:
    name: str
    statement: str
    is_mergetree: bool


class CopyTablesResponse(TypedDict, total=False):
    source_host: str
    target_host: str
    tables: str
    cluster_name: str
    dry_run: bool
    incomplete_hosts: dict[str, str]
    verified: int


def get_create_table_statements(
    tables: Sequence[str],
    source_connection: ClickhousePool,
    source_database: str,
    cluster_name: str | None,
) -> Sequence[TableStatement]:
    table_statements = []

    for table in tables:
        db_table = f"{source_database}.{table}"
        table_statement = source_connection.execute(f"SHOW CREATE TABLE {db_table}").results[0][0]

        assert isinstance(table_statement, str)

        table_engine = source_connection.execute(
            f"SELECT engine FROM system.tables WHERE name = '{table}'"
        ).results[0][0]

        if table_statement.startswith("CREATE MATERIALIZED VIEW"):
            table_statement = table_statement.replace(
                "CREATE MATERIALIZED VIEW",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS",
            )
            is_mergetree = False
        else:
            table_statement = table_statement
            table_statement = table_statement.replace(
                "CREATE TABLE",
                "CREATE TABLE IF NOT EXISTS",
            )
            table_engine = source_connection.execute(
                f"SELECT engine FROM system.tables WHERE name = '{table}'"
            ).results[0][0]
            is_mergetree = "MergeTree" in table_engine

        if cluster_name:
            table_statement = table_statement.replace(
                db_table, f"{db_table} ON CLUSTER {escape_string(cluster_name)}"
            )

        table_statements.append(
            TableStatement(name=table, statement=table_statement, is_mergetree=is_mergetree)
        )

    return table_statements


def get_tables(connection: ClickhousePool) -> Sequence[str]:
    results = connection.execute("SHOW TABLES").results
    tables = [result[0] for result in results]
    return tables


def verify_tables_on_replicas(
    connection: ClickhousePool,
    cluster_name: str | None,
    database_name: str,
    table_names: Sequence[str],
) -> tuple[MutableMapping[str, list[str]], int]:
    """
    Checks that the tables we have copied are present on all hosts.
    Returns a count of the verified hosts (host that have all the
    correct tables) and a mapping of hosts to the missing tables
    if the expected created tables are missing.
    """
    if cluster_name:
        from_clause = f"FROM clusterAllReplicas({escape_string(cluster_name)}, system.tables)"
    else:
        from_clause = "FROM system.tables"

    query = f"""
    SELECT
        hostName() as host,
        groupArray(name) as table_name
    {from_clause}
    WHERE database = {escape_string(database_name)}
    GROUP BY host
    ORDER BY host
    """

    results = connection.execute(query).results

    created_tables = set(table_names)
    missing_host_tables: MutableMapping[str, list[str]] = {}
    verified_hosts_num = 0
    for row in results:
        host = row[0]
        tables_on_host = set(row[1])
        # its possible that a node has extra tables so we only check that the
        # expected created tables are on the host, instead comparing table counts
        missing_tables = [t for t in created_tables if t not in tables_on_host]
        if missing_tables:
            missing_host_tables[host] = missing_tables
        else:
            verified_hosts_num += 1
    return missing_host_tables, verified_hosts_num


def copy_tables(
    source_host: str,
    storage_name: str,
    dry_run: bool,
    target_host: str | None = None,
    skip_on_cluster: bool = False,
    cluster_name_override: str | None = None,
) -> CopyTablesResponse:
    # Table copies can run long, so use the unbounded INTERNAL profile rather
    # than the 30s user-read QUERY profile.
    settings = ClickhouseClientSettings.INTERNAL
    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database_name = cluster.get_database()
    parsed_target: tuple[str, int] | None = None
    if target_host:
        parsed_target = parse_target_host(target_host)
        assert_target_host_allowed(parsed_target[0], parsed_target[1], cluster, storage_name)
    source_connection = get_clusterless_node_connection(
        source_host,
        _http_port_for_host(source_host, cluster),
        storage_name,
        client_settings=settings,
    )

    if skip_on_cluster:
        cluster_name = None
    elif cluster_name_override:
        cluster_name = validate_cluster_name(cluster_name_override)
    elif not cluster.is_single_node():
        cluster_name = storage.get_cluster().get_clickhouse_cluster_name()
        assert cluster_name, "Missing cluster name for ON CLUSTER create statement"
    else:
        cluster_name = None

    tables = get_tables(source_connection)
    table_statements = get_create_table_statements(
        tables, source_connection, database_name, cluster_name
    )

    mergetree_tables = [ts for ts in table_statements if ts.is_mergetree]
    non_mergetree_tables = [ts for ts in table_statements if not ts.is_mergetree]

    ordered_table_names = [ts.name for ts in mergetree_tables] + [
        ts.name for ts in non_mergetree_tables
    ]

    resp: CopyTablesResponse = {
        "source_host": source_host,
        "tables": ",".join(ordered_table_names),
        "cluster_name": cluster_name or "no cluster",
        "dry_run": dry_run,
    }
    if parsed_target:
        resp["target_host"] = parsed_target[0]

    if dry_run:
        return resp

    if parsed_target:
        host, port = parsed_target
        if target_host_is_allowlisted(host, port):
            target_connection = get_clusterless_node_connection(
                host,
                port,
                storage_name,
                client_settings=settings,
                validate_node=False,
            )
        else:
            target_connection = get_clusterless_node_connection(
                host,
                _http_port_for_host(host, cluster),
                storage_name,
                client_settings=settings,
            )
    else:
        target_connection = source_connection

    for ts in mergetree_tables:
        target_connection.command(ts.statement)

    for ts in non_mergetree_tables:
        target_connection.command(ts.statement)

    # Verify tables were created on all replicas
    missing_tables_by_host, verified_hosts_num = verify_tables_on_replicas(
        target_connection, cluster_name, database_name, ordered_table_names
    )

    resp["incomplete_hosts"] = {
        host: ",".join(tables) for host, tables in missing_tables_by_host.items()
    }
    resp["verified"] = verified_hosts_num
    return resp
