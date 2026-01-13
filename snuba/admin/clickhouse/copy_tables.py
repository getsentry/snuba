from dataclasses import dataclass
from typing import MutableMapping, Optional, Sequence

from snuba.admin.clickhouse.common import _get_storage, get_clusterless_node_connection
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings


@dataclass
class TableStatement:
    name: str
    statement: str
    is_mergetree: bool


def get_create_table_statements(
    tables: Sequence[str], source_connection: ClickhousePool, cluster_name: Optional[str]
) -> Sequence[TableStatement]:
    table_statements = []
    source_database = "default"

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
                db_table, f"{db_table} ON CLUSTER {cluster_name} "
            )

        table_statements.append(
            TableStatement(name=table, statement=table_statement, is_mergetree=is_mergetree)
        )

    return table_statements


def get_tables(connection: ClickhousePool) -> Sequence[str]:
    results = connection.execute("SHOW TABLES").results
    tables = [result[0] for result in results]
    return tables


def copy_tables(
    source_host: str,
    target_host: str,
    storage_name: str,
    dry_run: bool,
) -> MutableMapping[str, str | bool]:
    settings = ClickhouseClientSettings.QUERY
    source_connection = get_clusterless_node_connection(
        source_host, 9000, storage_name, client_settings=settings
    )
    target_connection = get_clusterless_node_connection(
        target_host, 9000, storage_name, client_settings=settings
    )

    storage = _get_storage(storage_name)
    cluster_name = storage.get_cluster().get_clickhouse_cluster_name()

    assert cluster_name, "Missing cluster name for ON CLUSTER create statement "

    tables = get_tables(source_connection)
    table_statements = get_create_table_statements(tables, source_connection, cluster_name)

    mergetree_tables = [ts for ts in table_statements if ts.is_mergetree]
    non_mergetree_tables = [ts for ts in table_statements if not ts.is_mergetree]

    ordered_table_names = [ts.name for ts in mergetree_tables] + [
        ts.name for ts in non_mergetree_tables
    ]

    resp: MutableMapping[str, str | bool] = {
        "source_host": source_host,
        "target_host": target_host,
        "tables": ",".join(ordered_table_names),
        "cluster_name": cluster_name,
        "dry_run": dry_run,
    }

    if dry_run:
        return resp

    for ts in mergetree_tables:
        target_connection.execute(ts.statement)

    for ts in non_mergetree_tables:
        target_connection.execute(ts.statement)

    return resp
