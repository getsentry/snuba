from typing import Optional

import click

from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry

TRANSACTIONS_DELETE_QUERY = """
ALTER TABLE transactions_local
DELETE WHERE
    project_id = %(project_id)d
    AND toMonday(finish_ts) = '%(to_monday_ts_lower)s'
"""

# FIXME: remove once I document how to get partition timestamps in an
# easily repeatable way

# fetch timestamps with:
# SELECT partition, count(*)
# from system.parts
# where table='transactions_local'
#    and active=1
# group by partition
# order by partition
TEST_TIMESTAMPS = [
    "2022-08-29",
    "2022-09-05",
    "2022-09-12",
    "2022-09-19",
    "2022-09-26",
    "2022-10-03",
    "2022-10-10",
    "2022-10-17",
    "2022-10-24",
    "2022-10-31",
    "2022-11-07",
    "2022-11-14",
    "2022-11-21",
    "2022-11-28",
]

QUERY_PARAMS = [{"project_id": 2, "to_monday_ts_lower": ts} for ts in TEST_TIMESTAMPS]


@click.command()
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
def data_delete(
    *,
    dry_run: bool,
    storage_name: str,
    log_level: Optional[str] = None,
) -> None:
    """
    Script the removal of data from tables using a customer-input ALTER DELETE command,
    supporting automatically connecting to one replica on each shard in a cluster.

    Optionally, add a verify command to see that each partition has 0 rows remaining.
    """

    setup_logging(log_level)
    setup_sentry()

    from snuba.data_delete import data_delete as delete_real
    from snuba.data_delete import logger

    storage = get_writable_storage(StorageKey(storage_name))
    (
        clickhouse_user,
        clickhouse_password,
    ) = storage.get_cluster().get_credentials()

    cluster = storage.get_cluster()

    result = delete_real(
        cluster,
        clickhouse_user,
        clickhouse_password,
        TRANSACTIONS_DELETE_QUERY,
        QUERY_PARAMS,
        dry_run,
    )

    logger.info("data_delete.after", result=result)
