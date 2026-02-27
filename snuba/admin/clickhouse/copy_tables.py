from dataclasses import dataclass
from typing import MutableMapping, Optional, Sequence, Tuple, TypedDict

from snuba.admin.clickhouse.common import _get_storage, get_clusterless_node_connection
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings


@dataclass
class TableStatement:
    name: str
    statement: str
    is_mergetree: bool


class CopyTablesResponse(TypedDict, total=False):
    source_host: str
    tables: str
    cluster_name: str
    dry_run: bool
    incomplete_hosts: dict[str, str]
    verified: int


def get_create_table_statements(
    tables: Sequence[str],
    source_connection: ClickhousePool,
    source_database: str,
    cluster_name: Optional[str],
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
            is_mergetree = True if "MergeTree" in table_engine else False

        if cluster_name:
            table_statement = table_statement.replace(
                db_table, f"{db_table} ON CLUSTER '{cluster_name}'"
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
    cluster_name: Optional[str],
    database_name: str,
    table_names: Sequence[str],
) -> Tuple[MutableMapping[str, list[str]], int]:
    """
    Checks that the tables we have copied are present on all hosts.
    Returns a count of the verified hosts (host that have all the
    correct tables) and a mapping of hosts to the missing tables
    if the expected created tables are missing.
    """
    if cluster_name:
        from_clause = f"FROM clusterAllReplicas('{cluster_name}', system.tables)"
    else:
        from_clause = "FROM system.tables"

    query = f"""
    SELECT
        hostName() as host,
        groupArray(name) as table_name
    {from_clause}
    WHERE database = '{database_name}'
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
    target_host: Optional[str] = None,
) -> CopyTablesResponse:
    settings = ClickhouseClientSettings.QUERY
    source_connection = get_clusterless_node_connection(
        source_host, 9000, storage_name, client_settings=settings
    )

    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database_name = cluster.get_database()

    if not cluster.is_single_node():
        cluster_name = storage.get_cluster().get_clickhouse_cluster_name()

        assert cluster_name, "Missing cluster name for ON CLUSTER create statement "
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

    if dry_run:
        return resp

    if target_host:
        target_connection = get_clusterless_node_connection(
            target_host, 9000, storage_name, client_settings=settings
        )
    else:
        target_connection = source_connection

    for ts in mergetree_tables:
        target_connection.execute(ts.statement)

    for ts in non_mergetree_tables:
        target_connection.execute(ts.statement)

    # Verify tables were created on all replicas
    missing_tables_by_host, verified_hosts_num = verify_tables_on_replicas(
        target_connection, cluster_name, database_name, ordered_table_names
    )

    resp["incomplete_hosts"] = {
        host: ",".join(tables) for host, tables in missing_tables_by_host.items()
    }
    resp["verified"] = verified_hosts_num
    return resp
