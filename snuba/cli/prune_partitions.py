import logging
from collections.abc import Callable, Sequence
from datetime import timedelta

import click

from snuba import environment
from snuba.clickhouse.pool import ClickhousePool
from snuba.clickhouse.prune_partitions import (
    DEFAULT_BATCH_SIZE,
    ForgetError,
    TablePruneResult,
    prune_table,
    today,
)
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseNode,
    build_pool,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger("snuba.prune_partitions")


@click.command()
@click.option(
    "--storage",
    "storage_names",
    multiple=True,
    required=True,
    help="Storage whose local table should be pruned. May be repeated.",
)
@click.option("--clickhouse-host", help="ClickHouse storage node to prune.")
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
    help="Forget partitions. The default only reports which partitions are eligible.",
)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=DEFAULT_BATCH_SIZE,
    show_default=True,
    help="Partitions forgotten per batch. Eligibility is re-checked before each batch.",
)
@click.option(
    "--max-forgets",
    type=click.IntRange(min=0),
    default=50,
    show_default=True,
    help="Maximum partitions forgotten in this run, across all storages.",
)
@click.option(
    "--retention-margin-days",
    type=click.IntRange(min=0),
    default=14,
    show_default=True,
    help="Days past the end of retention before a partition is eligible.",
)
@click.option("--log-level", help="Logging level to use.")
def prune_partitions(
    *,
    storage_names: Sequence[str],
    clickhouse_host: str | None,
    clickhouse_port: int | None,
    clickhouse_secure: bool,
    clickhouse_ca_certs: str | None,
    clickhouse_verify: bool,
    execute: bool,
    batch_size: int,
    max_forgets: int,
    retention_margin_days: int,
    log_level: str | None,
) -> None:
    """Forget block_numbers znodes of dead partitions on one storage node.

    A partition is forgotten only when it is past retention plus the margin and
    no replica of the node's shard has parts, detached parts, replication queue
    entries, unfinished mutations or block locks for it. Tables that are not
    replicated, or do not have both enable_block_number_column and
    enable_block_offset_column set, are skipped.
    """
    setup_logging(log_level)
    setup_sentry()

    if (clickhouse_host is None) != (clickhouse_port is None):
        raise click.UsageError("--clickhouse-host and --clickhouse-port must be provided together")

    storages = []
    for storage_name in storage_names:
        try:
            storages.append(get_storage(StorageKey(storage_name)))
        except (KeyError, ValueError) as error:
            raise click.BadParameter(
                f"Unknown storage: {storage_name}", param_hint="--storage"
            ) from error

    metrics = MetricsWrapper(environment.metrics, "prune_partitions")
    mode = "EXECUTE" if execute else "DRY RUN"
    remaining = max_forgets

    for storage in storages:
        cluster = storage.get_cluster()
        database = cluster.get_database()
        schema = storage.get_schema()
        assert isinstance(schema, TableSchema)
        table = schema.get_local_table_name()
        user, password = cluster.get_credentials()

        connection: ClickhousePool
        if clickhouse_host is not None and clickhouse_port is not None:
            connection = build_pool(
                ClickhouseClientSettings.CLEANUP,
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
            connection = cluster.get_query_connection(ClickhouseClientSettings.CLEANUP)

        cluster_name = None if cluster.is_single_node() else cluster.get_clickhouse_cluster_name()
        try:
            result = prune_table(
                connection,
                database=database,
                table=table,
                cluster_name=cluster_name,
                metrics=metrics,
                today=today(),
                retention_margin=timedelta(days=retention_margin_days),
                batch_size=batch_size,
                max_forgets=remaining,
                dry_run=not execute,
                on_partition_forgotten=_log_forgotten(database, table),
            )
        except ForgetError as error:
            _report(mode, database, error.result)
            raise click.ClickException(
                f"{error}. An eligibility check was wrong; stopping."
            ) from error

        _report(mode, database, result)
        remaining -= result.forgotten_count


def _log_forgotten(database: str, table: str) -> Callable[[str], None]:
    def log(partition_id: str) -> None:
        logger.info("[EXECUTE] Forgot partition %s of %s.%s", partition_id, database, table)

    return log


def _report(mode: str, database: str, result: TablePruneResult) -> None:
    if result.skipped_reason is not None:
        logger.warning(
            "[%s] Skipped %s.%s: %s", mode, database, result.table, result.skipped_reason
        )
        if not result.partitions:
            return

    for p in result.partitions:
        logger.info(
            "[%s] shard=%s table=%s partition_id=%s category=%s eligible=%s "
            "forgotten=%s failed_checks=%s",
            mode,
            result.shard,
            result.table,
            p.partition_id,
            p.category.value,
            p.eligible,
            p.forgotten,
            ",".join(p.blockers),
        )

    logger.info(
        "[%s] %s.%s shard=%s: %d znode(s), %d eligible, %d forgotten, expected scan size %d",
        mode,
        database,
        result.table,
        result.shard,
        len(result.partitions),
        result.eligible_count,
        result.forgotten_count,
        result.expected_scan_size,
    )
