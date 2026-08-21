from collections.abc import Sequence

import click

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry


@click.command()
@click.argument("source_table")
@click.argument("destination_table")
@click.option(
    "--storage",
    "storage_name",
    required=True,
    help="Storage whose ClickHouse cluster and database contain both tables.",
)
@click.option("--clickhouse-host", help="ClickHouse server to write to.")
@click.option("--clickhouse-port", type=int, help="ClickHouse port identifying the target node.")
@click.option(
    "--clickhouse-secure/--no-clickhouse-secure",
    default=False,
    help="Use an encrypted ClickHouse connection.",
)
@click.option("--clickhouse-ca-certs", help="Optional path to a certificates directory.")
@click.option(
    "--clickhouse-verify/--no-clickhouse-verify",
    default=False,
    help="Verify the ClickHouse TLS certificate.",
)
@click.option(
    "--execute/--dry-run",
    default=False,
    help="Attach partitions. The default only lists partitions that would be attached.",
)
@click.option(
    "--partition-id",
    help="Attach only this ClickHouse partition ID instead of discovering partitions.",
)
@click.option("--log-level", help="Logging level to use.")
def attach_partitions(
    source_table: str,
    destination_table: str,
    *,
    storage_name: str,
    clickhouse_host: str | None,
    clickhouse_port: int | None,
    clickhouse_secure: bool,
    clickhouse_ca_certs: str | None,
    clickhouse_verify: bool,
    execute: bool,
    partition_id: str | None,
    log_level: str | None,
) -> None:
    """Attach SOURCE_TABLE partitions to DESTINATION_TABLE one at a time.

    Table names are unqualified and must be in the database used by STORAGE.
    In discovery mode, existing destination partitions are skipped. A specific
    partition ID is attached directly. The source data is not removed.
    """
    from snuba.clickhouse.partition_management import (
        attach_partition_from_table,
        attach_partitions_from_table,
        check_clickhouse_health,
    )
    from snuba.clickhouse.pool import ClickhousePool
    from snuba.clusters.cluster import ClickhouseNode, build_pool

    setup_logging(log_level)
    setup_sentry()

    if (clickhouse_host is None) != (clickhouse_port is None):
        raise click.UsageError("--clickhouse-host and --clickhouse-port must be provided together")

    try:
        storage = get_storage(StorageKey(storage_name))
    except ValueError as error:
        raise click.BadParameter(
            f"Unknown storage: {storage_name}", param_hint="--storage"
        ) from error

    cluster = storage.get_cluster()
    database = cluster.get_database()
    user, password = cluster.get_credentials()

    connection: ClickhousePool
    if clickhouse_host is not None and clickhouse_port is not None:
        connection = build_pool(
            ClickhouseClientSettings.MIGRATE,
            ClickhouseNode(clickhouse_host, clickhouse_port),
            user,
            password,
            database,
            secure=clickhouse_secure,
            ca_certs=clickhouse_ca_certs,
            verify=clickhouse_verify,
        )
    elif not cluster.is_single_node():
        raise click.UsageError(
            "Provide --clickhouse-host and --clickhouse-port for a multi-node cluster"
        )
    else:
        connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

    partition_ids: Sequence[str]
    if partition_id is not None:
        partition_ids = [partition_id]
        if execute:
            check_clickhouse_health(connection)
            attach_partition_from_table(
                connection,
                database,
                source_table,
                destination_table,
                partition_id,
            )
            click.echo(f"Attached partition {partition_id}")
    else:
        partition_ids = attach_partitions_from_table(
            connection,
            database,
            source_table,
            destination_table,
            dry_run=not execute,
            on_partition_attached=lambda attached_partition_id: click.echo(
                f"Attached partition {attached_partition_id}"
            ),
        )

    if not execute:
        for partition_id in partition_ids:
            click.echo(f"Would attach partition {partition_id}")

    action = "Attached" if execute else "Would attach"
    click.echo(f"{action} {len(partition_ids)} partition(s)")
