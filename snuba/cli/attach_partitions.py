import logging
from collections.abc import Sequence

import click

from snuba.clickhouse.partition_management import (
    PartitionBoundaryError,
    attach_partition_from_table,
    attach_partitions_from_table,
    build_health_check,
    get_partition_boundaries,
)
from snuba.clickhouse.pool import ClickhousePool
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseNode,
    build_pool,
)
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry

logger = logging.getLogger("snuba.attach_partitions")


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
@click.option(
    "--health-check-query",
    help=(
        "Health check run before each attach, replacing the default SELECT 1. "
        "May reference %(partition_start)s, bound to the start of the boundary "
        "of the partition about to be attached, and %(retention_days)s when the "
        "partition key carries one. Scope on both to identify a single partition "
        "of a table partitioned by (retention_days, toMonday(timestamp)). The "
        "destination is unhealthy if the query fails or returns no rows or a "
        "falsy first value. The partition key must include a date or datetime "
        "component."
    ),
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
    health_check_query: str | None,
    log_level: str | None,
) -> None:
    """Attach SOURCE_TABLE partitions to DESTINATION_TABLE one at a time.

    Table names are unqualified and must be in the database used by STORAGE.
    In discovery mode, existing destination partitions are skipped. A specific
    partition ID is attached directly. The source data is not removed.
    """
    setup_logging(log_level)
    setup_sentry()

    if (clickhouse_host is None) != (clickhouse_port is None):
        raise click.UsageError("--clickhouse-host and --clickhouse-port must be provided together")

    try:
        storage = get_storage(StorageKey(storage_name))
    except (KeyError, ValueError) as error:
        # StorageKey accepts any string, so an unknown storage only surfaces as
        # a KeyError from the registry lookup.
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

    source = f"{database}.{source_table}"
    destination = f"{database}.{destination_table}"
    mode = "EXECUTE" if execute else "DRY RUN"
    logger.info("[%s] Attaching partitions from %s to %s", mode, source, destination)
    if not execute:
        logger.info("[DRY RUN] No partitions will be attached. Pass --execute to attach.")

    partition_ids: Sequence[str]
    try:
        if partition_id is not None:
            partition_ids = [partition_id]
            if execute:
                boundaries = (
                    get_partition_boundaries(connection, database, source_table)
                    if health_check_query is not None
                    else None
                )
                build_health_check(connection, health_check_query, boundaries)(partition_id)
                attach_partition_from_table(
                    connection,
                    database,
                    source_table,
                    destination_table,
                    partition_id,
                )
                logger.info(
                    "Attached partition %s from %s to %s", partition_id, source, destination
                )
        else:
            partition_ids = attach_partitions_from_table(
                connection,
                database,
                source_table,
                destination_table,
                health_check_query=health_check_query,
                dry_run=not execute,
                on_partition_attached=lambda attached_partition_id: logger.info(
                    "Attached partition %s from %s to %s",
                    attached_partition_id,
                    source,
                    destination,
                ),
            )
    except PartitionBoundaryError as error:
        raise click.UsageError(str(error)) from error

    if not execute:
        for partition_id in partition_ids:
            logger.info(
                "Would attach partition %s from %s to %s", partition_id, source, destination
            )

    action = "Attached" if execute else "Would attach"
    logger.info(
        "[%s] %s %d partition(s) from %s to %s",
        mode,
        action,
        len(partition_ids),
        source,
        destination,
    )
