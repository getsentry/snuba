from typing import Optional

import click

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.factory import DATASET_NAMES, get_dataset
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import STORAGES, get_storage
from snuba.environment import setup_logging


@click.command()
@click.option(
    "--clickhouse-host", help="Clickhouse server to write to.",
)
@click.option(
    "--clickhouse-port", type=int, help="Clickhouse native port to write to.",
)
@click.option("--database", default="default", help="Name of the database to target.")
@click.option(
    "--dataset",
    "dataset_name",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
@click.option(
    "--storage",
    "storage_name",
    default="events",
    type=click.Choice([storage_key.value for storage_key in STORAGES.keys()]),
    help="The storage to target",
)
@click.option("--log-level", help="Logging level to use.")
def optimize(
    *,
    clickhouse_host: Optional[str],
    clickhouse_port: Optional[int],
    database: str,
    dataset_name: str,
    storage_name: str,
    log_level: Optional[str] = None,
) -> None:
    from datetime import datetime
    from snuba.clickhouse.native import ClickhousePool
    from snuba.optimize import run_optimize, logger

    setup_logging(log_level)

    storage: ReadableTableStorage

    if dataset_name:
        dataset = get_dataset(dataset_name)
        writable_storage = dataset.get_writable_storage()
        assert writable_storage is not None, "Dataset has no writable storage"
        storage = writable_storage
    else:
        storage_key = StorageKey(storage_name)
        storage = get_storage(storage_key)

    (clickhouse_user, clickhouse_password,) = storage.get_cluster().get_credentials()

    schemas = set()
    read_schema = storage.get_schemas().get_read_schema()
    write_schema = storage.get_schemas().get_write_schema()

    if read_schema:
        schemas.add(read_schema)

    if write_schema:
        schemas.add(write_schema)

    tables = {
        schema.get_local_table_name()
        for schema in schemas
        if isinstance(schema, TableSchema)
    }

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # TODO: In distributed mode, optimize currently must be run once for each node
    # with the host and port of that node provided via the CLI. In the future,
    # passing this information won't be necessary, and running this command once
    # will ensure that optimize is performed on all of the individual nodes for
    # that cluster.
    if clickhouse_host and clickhouse_port and database:
        connection = ClickhousePool(
            clickhouse_host,
            clickhouse_port,
            clickhouse_user,
            clickhouse_password,
            database,
            send_receive_timeout=ClickhouseClientSettings.OPTIMIZE.value.timeout,
        )
    elif not storage.get_cluster().is_single_node():
        raise click.ClickException("Provide Clickhouse host and port for optimize")
    else:
        connection = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.OPTIMIZE
        )

    for table in tables:
        num_dropped = run_optimize(connection, database, table, before=today)
        logger.info("Optimized %s partitions on %s" % (num_dropped, clickhouse_host))
