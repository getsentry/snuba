import logging
from collections.abc import Callable, Sequence

from snuba.clickhouse.escaping import escape_identifier
from snuba.clickhouse.pool import ClickhousePool

logger = logging.getLogger("snuba.clickhouse.partition_management")

HealthCheck = Callable[[], None]
PartitionAttached = Callable[[str], None]


def get_active_partition_ids(
    clickhouse: ClickhousePool, database: str, table: str
) -> Sequence[str]:
    """Return stable ClickHouse partition IDs for the active parts in a table."""
    response = clickhouse.execute(
        """
        SELECT DISTINCT partition_id
        FROM system.parts
        WHERE database = %(database)s
        AND table = %(table)s
        AND active = 1
        ORDER BY partition_id
        """,
        {"database": database, "table": table},
    )
    return [partition_id for (partition_id,) in response.results]


def check_clickhouse_health(clickhouse: ClickhousePool) -> None:
    """Raise when ClickHouse cannot serve a minimal query."""
    clickhouse.execute("SELECT 1")


def attach_partition_from_table(
    clickhouse: ClickhousePool,
    database: str,
    source_table: str,
    destination_table: str,
    partition_id: str,
) -> None:
    """Attach one source partition to the destination, leaving the source intact."""
    if source_table == destination_table:
        raise ValueError("Source and destination tables must be different")

    escaped_database = escape_identifier(database)
    escaped_source = escape_identifier(source_table)
    escaped_destination = escape_identifier(destination_table)
    assert escaped_database is not None
    assert escaped_source is not None
    assert escaped_destination is not None

    logger.info(
        "Attaching partition %s from %s.%s to %s.%s",
        partition_id,
        database,
        source_table,
        database,
        destination_table,
    )
    clickhouse.execute(
        f"ALTER TABLE {escaped_database}.{escaped_destination} "
        f"ATTACH PARTITION ID %(partition_id)s FROM "
        f"{escaped_database}.{escaped_source}",
        {"partition_id": partition_id},
    )


def attach_partitions_from_table(
    clickhouse: ClickhousePool,
    database: str,
    source_table: str,
    destination_table: str,
    *,
    health_check: HealthCheck | None = None,
    on_partition_attached: PartitionAttached | None = None,
    dry_run: bool = False,
) -> Sequence[str]:
    """
    Attach active source partitions that are not active on the destination.

    Partitions are attached serially. The health check runs before every attach,
    which gates the first operation and every operation following an attach. A
    failed check aborts the process without attempting the next partition.

    ClickHouse's ``ATTACH PARTITION ... FROM`` leaves the source partition in
    place. Re-running this function is safe because active destination partition
    IDs are skipped.
    """
    if source_table == destination_table:
        raise ValueError("Source and destination tables must be different")

    source_partition_ids = get_active_partition_ids(clickhouse, database, source_table)
    destination_partition_ids = set(
        get_active_partition_ids(clickhouse, database, destination_table)
    )
    pending_partition_ids = [
        partition_id
        for partition_id in source_partition_ids
        if partition_id not in destination_partition_ids
    ]

    if dry_run:
        return pending_partition_ids

    check_health = health_check or (lambda: check_clickhouse_health(clickhouse))
    attached_partition_ids = []
    for partition_id in pending_partition_ids:
        check_health()
        attach_partition_from_table(
            clickhouse,
            database,
            source_table,
            destination_table,
            partition_id,
        )
        attached_partition_ids.append(partition_id)
        if on_partition_attached is not None:
            on_partition_attached(partition_id)

    return attached_partition_ids
