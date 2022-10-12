import argparse
import re

from clickhouse_driver import Client

SETTINGS = {
    "load_balancing": "in_order",
    "replication_alter_partitions_sync": 2,
    "mutations_sync": 2,
    "database_atomic_wait_for_drop_and_detach_synchronously": 1,
}


def _get_client(host, port, user, password, database):
    return Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        settings=SETTINGS,
    )


def verify_zk_replica_path(client, database, tables):
    """
    Before we copy over table statements from other nodes, we need to make sure
    that the current zookeeper paths use the macros defined and align with the path
    that is generated in the create table statement.

    """
    print("Looking up macros...")
    ((_, replica), (_, shard)) = client.execute(
        "SELECT macro, substitution FROM system.macros"
    )
    print(f"Found replica: {replica} for shard: {shard}")

    for table in tables:
        print(f"Verifying zk replica path for table: {table}...")
        ((replica_path,),) = client.execute(
            f"SELECT replica_path FROM system.replicas where table = '{table}'"
        )

        ((curr_create_table_statement,),) = source_client.execute(
            f"SHOW CREATE TABLE {database}.{table}"
        )
        match = re.search("\/clickhouse(.*,)", curr_create_table_statement)
        create_table_path = match.group(0)[:-2].replace("{shard}", shard)

        built_replica_path = f"{create_table_path}/replicas/{replica}"

        assert (
            built_replica_path == replica_path
        ), f"{built_replica_path} should match zk path: {replica_path}"
        print(f"Zookeeper replica paths verified for table: {table} ! :)")


def copy_tables(source_client, new_client, tables, database, dry_run):

    verify_zk_replica_path(source_client, args.database, tables)

    if not tables:
        tables = [result[0] for result in source_client.execute("SHOW TABLES").results]

    show_table_statments = {}

    for name in tables:
        ((curr_create_table_statement,),) = source_client.execute(
            f"SHOW CREATE TABLE {database}.{name}"
        )
        show_table_statments[name] = curr_create_table_statement

    for table_name, statement in show_table_statments.items():
        print(f"creating {table_name}...")
        if dry_run:
            print(f"create table statement: \n {statement}")
            return
        new_client.execute(statement)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy ClickHouse Tables")
    parser.add_argument("--source-host", default="localhost", help="clickhouse host")
    parser.add_argument("--source-port", help="source port")
    parser.add_argument("--new-host", default="localhost")
    parser.add_argument("--new-port", help="new port")
    parser.add_argument("--user", default="default")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="default")
    parser.add_argument("--tables")
    parser.add_argument("--dry-run", type=bool, default=False)

    args = parser.parse_args()

    source_client = _get_client(
        host=args.source_host,
        port=args.source_port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    new_client = _get_client(
        host=args.source_host,
        port=args.source_port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    tables = args.tables.split(",")

    copy_tables(source_client, new_client, tables, args.database, args.dry_run)
