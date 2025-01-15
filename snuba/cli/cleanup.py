from typing import Optional

import click

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry


@click.command()
@click.option(
    "--clickhouse-host",
    help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port",
    type=int,
    help="Clickhouse native port to write to.",
)
@click.option(
    "--clickhouse-secure",
    type=bool,
    default=False,
    help="If true, an encrypted connection will be used",
)
@click.option(
    "--clickhouse-ca-certs",
    type=str,
    default=None,
    help="An optional path to certificates directory.",
)
@click.option(
    "--clickhouse-verify",
    type=bool,
    default=False,
    help="Verify ClickHouse SSL cert.",
)
@click.option(
    "--dry-run",
    type=bool,
    default=True,
    help="If true, only print which partitions would be dropped.",
)
@click.option(
    "--storage",
    "storage_name",
    type=click.Choice(["errors", "transactions"]),
    help="The storage to target",
    required=True,
)
@click.option("--log-level", help="Logging level to use.")
def cleanup(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    clickhouse_secure: bool,
    clickhouse_ca_certs: Optional[str],
    clickhouse_verify: Optional[bool],
    dry_run: bool,
    storage_name: str,
    log_level: Optional[str] = None,
) -> None:
    """
    Deletes stale partitions for ClickHouse tables
    """

    setup_logging(log_level)
    setup_sentry()

    from snuba.cleanup import logger, run_cleanup
    from snuba.clickhouse.native import ClickhousePool

    storage = get_writable_storage(StorageKey(storage_name))

    (
        clickhouse_user,
        clickhouse_password,
    ) = storage.get_cluster().get_credentials()

    cluster = storage.get_cluster()
    database = cluster.get_database()

    if clickhouse_host and clickhouse_port:
        connection = ClickhousePool(
            clickhouse_host,
            clickhouse_port,
            clickhouse_user,
            clickhouse_password,
            database,
            clickhouse_secure,
            clickhouse_ca_certs,
            clickhouse_verify,
        )
    elif not cluster.is_single_node():
        raise click.ClickException("Provide ClickHouse host and port for cleanup")
    else:
        connection = cluster.get_query_connection(ClickhouseClientSettings.CLEANUP)

    num_dropped = run_cleanup(connection, storage, database, dry_run=dry_run)
    logger.info("Dropped %s partitions on %s" % (num_dropped, cluster))
