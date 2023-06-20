#!/usr/bin/env python3

import argparse
import re
from collections import OrderedDict
from typing import Mapping, Optional, Sequence

from clickhouse_driver import Client


def _get_client(
    host: str, port: int, user: str, password: str, database: str
) -> Client:
    return Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


def get_regex_match(curr_create_table_statement: str) -> str:
    match = re.search("\/clickhouse([a-z\/\-{}_]*)", curr_create_table_statement)
    assert match is not None
    return match.group(0)


def verify_zk_replica_path(
    source_client: Client,
    curr_create_table_statement: str,
    table: str,
) -> None:
    """
    Before we copy over table statements from other nodes, we need to make sure
    that the current zookeeper paths use the macros defined and align with the path
    that is generated in the create table statement.

    """
    print("...looking up macros")
    ((_, source_replica), (_, source_shard)) = source_client.execute(
        "SELECT macro, substitution FROM system.macros"
    )
    print(f"...found replica: {source_replica} for shard: {source_shard}")

    print(f"...verifying zk replica path for table: {table}...")

    ((replica_path,),) = source_client.execute(
        f"SELECT replica_path FROM system.replicas where table = '{table}'"
    )

    match = get_regex_match(curr_create_table_statement)
    create_table_path = match.replace("{shard}", source_shard)

    built_replica_path = f"{create_table_path}/replicas/{source_replica}"

    assert (
        built_replica_path == replica_path
    ), f"{built_replica_path} should match zk path: {replica_path}"
    print(f"...zookeeper replica paths verified for table: {table} ! :)")


def verify_local_tables_exist_from_mv(
    target_client: Client, curr_create_table_statement: str, table: str
) -> None:
    """
    Verifies that the materialized views we want to create
    are being created after the local tables that the view reference.

    The create view statement will look something like:

    CREATE MATERIALIZED VIEW
        default.generic_metric_sets_aggregation_mv
    TO default.generic_metric_sets_local
    ...
    FROM default.generic_metric_sets_raw_local

    """
    print(f"...checking table statement: {table}")
    to_search = re.search("TO(.[.\w]*)", curr_create_table_statement)
    from_search = re.search("FROM(.[.\w]*)", curr_create_table_statement)

    assert to_search is not None
    assert from_search is not None

    # make sure we ditch the "default." before checking against SHOW TABLES
    _, to_local_table = to_search.group(1).strip().split(".")
    _, from_local_table = from_search.group(1).strip().split(".")

    print(f"...looking for local tables: {to_local_table}, {from_local_table} ")

    all_tables = [result[0] for result in target_client.execute("SHOW TABLES")]

    assert (
        to_local_table in all_tables
    ), f"{to_local_table} needs to be created before {table}"
    assert (
        from_local_table in all_tables
    ), f"{from_local_table} needs to be created before {table}"
    print("...local tables found, mv check complete !\n")


def copy_tables(
    source_client: Client,
    target_client: Client,
    source_database: str,
    target_database: str,
    execute: bool,
    tables: Optional[Sequence[str]],
) -> None:
    """
    When adding a replica to a clickhouse cluster, that node will not have any tables
    created yet. In order to get the up to date table statements, we need to copy the
    create table statements and run them on the new node(s).

    The source_client is the clickhouse node that you are copying the table statements
    from. The target_client is the clickhouse node is the node you want to create the
    tables on.

    The table engines that have some extra verification are materialized views, and
    any of the replicated tables that are listed in the clickhouse docs:
    https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication/

    By default running the script will only print out the CREATE TABLE statement, to
    actually have the CREATE TABLE statement run, use --execute.

    Consider doing one table at a time if you have tables that will take a while to
    replicate and might increase the cluster's normalized load considerably.
    """

    if not tables:
        tables = [result[0] for result in source_client.execute("SHOW TABLES")]

    show_table_statements = OrderedDict()
    show_mv_statements = OrderedDict()

    for name in tables:
        ((engine,),) = source_client.execute(
            f"SELECT engine FROM system.tables WHERE name = '{name}'"
        )

        ((curr_create_table_statement,),) = source_client.execute(
            f"SHOW CREATE TABLE {source_database}.{name}"
        )

        if engine == "MaterializedView":
            print("\nMaterialized View Check:")
            verify_local_tables_exist_from_mv(
                target_client, curr_create_table_statement, name
            )

        if engine.startswith("Replicated"):
            print("\nReplicated Table Check:")
            verify_zk_replica_path(source_client, curr_create_table_statement, name)

        if engine == "MaterializedView":
            show_mv_statements[name] = curr_create_table_statement
        else:
            show_table_statements[name] = curr_create_table_statement

    ((_, target_replica), (_, target_shard)) = target_client.execute(
        "SELECT macro, substitution FROM system.macros"
    )

    def execute_statements(statements: Mapping[str, str]) -> None:
        for table_name, statement in statements.items():
            if source_database != target_database:
                print(
                    f"Mismatched databases.. Replacing: {source_database}.{table_name} with {target_database}.{table_name}"
                )
                statement = statement.replace(
                    f"CREATE TABLE {source_database}.{table_name}",
                    f"CREATE TABLE {target_database}.{table_name}",
                )
            print(
                f"creating {table_name}... on replica: {target_replica}, shard: {target_shard}, database: {target_database}"
            )
            if execute:
                target_client.execute(statement)
                print(f"created {table_name} !")
            else:
                print(f"\ncreate table statement: \n {statement}\n")

    # make sure materialzied views are created after the local tables
    # they depend on
    execute_statements(show_table_statements)
    execute_statements(show_mv_statements)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy ClickHouse Tables")
    parser.add_argument(
        "--source-host",
        required=True,
        help="IP/name for node that tables should be copied from.",
    )
    parser.add_argument(
        "--source-port",
        help="Port for node that tables should be copied from.",
        default=9000,
    )
    parser.add_argument(
        "--target-host",
        required=True,
        help="IP/name for node that needs tables created.",
    )
    parser.add_argument(
        "--target-port", help="Port for node that needs tables created.", default=9000
    )
    parser.add_argument("--user", default="default")
    parser.add_argument("--password", default="")
    parser.add_argument("--source-database", default="default")
    parser.add_argument("--target-database", default="default")
    parser.add_argument("--tables", help="One or more tables separated by ','.")
    parser.add_argument(
        "--execute", action="store_true", help="Executes the create table statements."
    )

    args = parser.parse_args()

    source_client = _get_client(
        host=args.source_host,
        port=args.source_port,
        user=args.user,
        password=args.password,
        database=args.source_database,
    )

    target_client = _get_client(
        host=args.target_host,
        port=args.target_port,
        user=args.user,
        password=args.password,
        database=args.target_database,
    )

    tables = args.tables and args.tables.split(",")

    copy_tables(
        source_client,
        target_client,
        args.source_database,
        args.target_database,
        args.execute,
        tables,
    )
