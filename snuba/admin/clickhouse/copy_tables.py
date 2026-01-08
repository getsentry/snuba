from typing import MutableMapping, Optional

from snuba.admin.clickhouse.common import _get_storage, get_clusterless_node_connection
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings


def get_create_table_statement(
    source_connection: ClickhousePool,
    table: str,
    cluster_name: Optional[str],
) -> str:
    source_database = "default"
    table_statement = source_connection.execute(
        f"SHOW CREATE TABLE {source_database}.{table}"
    ).results[0][0]

    assert isinstance(table_statement, str)

    if table_statement.startswith("CREATE MATERIALIZED VIEW"):
        table_statement = table_statement.replace(
            "CREATE MATERIALIZED VIEW",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS",
        )
    else:
        table_statement = table_statement
        table_statement = table_statement.replace(
            "CREATE TABLE",
            "CREATE TABLE IF NOT EXISTS",
        )

    if cluster_name:
        db_table = f"{source_database}.{table}"
        table_statement = table_statement.replace(
            db_table, f"{db_table} ON CLUSTER {cluster_name} "
        )

    return table_statement


def copy_table(
    source_host: str,
    target_host: str,
    storage_name: str,
    table: str,
    dry_run: bool,
    on_cluster: bool,
) -> MutableMapping[str, str | bool]:
    settings = ClickhouseClientSettings.QUERY
    source_connection = get_clusterless_node_connection(
        source_host, 9000, storage_name, client_settings=settings
    )
    target_connection = get_clusterless_node_connection(
        target_host, 9000, storage_name, client_settings=settings
    )

    if on_cluster:
        storage = _get_storage(storage_name)
        cluster_name = storage.get_cluster().get_clickhouse_cluster_name()
    else:
        cluster_name = None

    statement = get_create_table_statement(source_connection, table, cluster_name)

    table_statement: MutableMapping[str, str | bool] = {
        "create_statement": statement,
        "source_host": source_host,
        "target_host": target_host,
        "table": table,
        "dry_run": dry_run,
        "on_cluster": on_cluster,
    }
    if dry_run:
        return table_statement
    else:
        target_connection.execute(statement)

    return table_statement
