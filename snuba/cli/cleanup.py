from typing import Optional

import click

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage, WRITABLE_STORAGES
from snuba.environment import setup_logging


@click.command()
@click.option(
    "--clickhouse-host", help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port", type=int, help="Clickhouse native port to write to.",
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
    default="events",
    type=click.Choice([storage_key.value for storage_key in WRITABLE_STORAGES.keys()]),
    help="The storage to target",
)
@click.option("--log-level", help="Logging level to use.")
def cleanup(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    dry_run: bool,
    storage_name: str,
    log_level: Optional[str] = None,
) -> None:
    """
    Deletes stale partitions for ClickHouse tables
    """

    setup_logging(log_level)

    from snuba.cleanup import run_cleanup, logger
    from snuba.clickhouse.native import ClickhousePool

    storage = get_writable_storage(StorageKey(storage_name))

    (clickhouse_user, clickhouse_password,) = storage.get_cluster().get_credentials()

    table = storage.get_table_writer().get_schema().get_local_table_name()

    database = storage.get_cluster().get_database()

    if clickhouse_host and clickhouse_port:
        connection = ClickhousePool(
            clickhouse_host,
            clickhouse_port,
            clickhouse_user,
            clickhouse_password,
            database,
        )
    elif not storage.get_cluster().is_single_node():
        raise click.ClickException("Provide ClickHouse host and port for cleanup")
    else:
        connection = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.CLEANUP
        )

    num_dropped = run_cleanup(connection, database, table, dry_run=dry_run)
    logger.info("Dropped %s partitions on %s" % (num_dropped, clickhouse_host))
