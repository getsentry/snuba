import logging
import re
from collections.abc import Callable, Sequence
from datetime import datetime

from snuba.clickhouse.escaping import escape_identifier
from snuba.clickhouse.pool import ClickhousePool

logger = logging.getLogger("snuba.clickhouse.partition_management")

HealthCheck = Callable[[str], None]
PartitionAttached = Callable[[str], None]

PARTITION_START_PARAM = "partition_start"
RETENTION_DAYS_PARAM = "retention_days"


class UnhealthyError(Exception):
    """Raised when a health check query reports an unhealthy destination."""


class PartitionBoundaryError(Exception):
    """Raised when a partition cannot be read as a time boundary."""


# Matches the date or datetime component of a partition value. ClickHouse
# renders tuple partitions as text such as ``(90,'2024-02-12')``, and quotes
# the date because it is not numeric.
_PARTITION_DATE = re.compile(r"'?(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2}))?'?")

# Matches a bare integer component of a partition value, which is how the
# retention_days column of an EAP partition key is rendered.
_PARTITION_INTEGER = re.compile(r"(?<![\w'-])(\d+)(?![\w'-])")

# Matches the ``%(name)s`` placeholders a health check query substitutes.
_QUERY_PARAMETER = re.compile(r"%\((\w+)\)s")

# Partition IDs for date-only keys, keyed by length because strptime accepts
# non-zero-padded components, which would otherwise let "%Y%m%d%H%M%S" match a
# shorter ID such as "2024021307" as 2024-02-01 03:00:07.
_PARTITION_ID_FORMATS = {
    4: "%Y",
    6: "%Y%m",
    8: "%Y%m%d",
    10: "%Y%m%d%H",
    14: "%Y%m%d%H%M%S",
}


def parse_partition_start(partition: str) -> datetime:
    """
    Read the start of a partition's time boundary from its ``system.parts``
    ``partition`` value.

    ClickHouse renders a tuple partition key as text, so a table partitioned by
    ``(retention_days, toMonday(timestamp))`` yields ``(90,'2024-02-12')`` and a
    table partitioned by ``toMonday(timestamp)`` yields ``2024-02-12``. Both
    expose the boundary even when the partition ID itself is hashed, which is
    what happens once a String column is part of the key.

    A purely numeric value is treated as a compact partition ID such as
    ``202402``. Values holding no date at all cannot be scoped and raise.
    """
    match = _PARTITION_DATE.search(partition)
    if match is not None:
        date_part, time_part = match.groups()
        if time_part is not None:
            return datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(date_part, "%Y-%m-%d")

    stripped = partition.strip("()'")
    date_format = _PARTITION_ID_FORMATS.get(len(stripped))
    if date_format is not None and stripped.isdigit():
        try:
            return datetime.strptime(stripped, date_format)
        except ValueError:
            pass

    raise PartitionBoundaryError(
        f"Cannot derive a partition start from partition {partition!r}. "
        "The partition key must include a date or datetime component for a "
        "partition-scoped health check."
    )


def _referenced_parameters(query: str) -> set[str]:
    """Return the ``%(name)s`` parameters a query references."""
    return set(_QUERY_PARAMETER.findall(query))


def parse_retention_days(partition: str) -> int | None:
    """
    Read ``retention_days`` from a ``system.parts`` ``partition`` value.

    EAP tables partition by ``(retention_days, toMonday(timestamp))``, which
    ClickHouse renders as ``(90,'2024-02-12')``. The date component is quoted,
    so the sole unquoted integer is ``retention_days``. Returns ``None`` when
    the partition key holds no such component, which covers date-only keys and
    compact partition IDs.
    """
    if _PARTITION_DATE.search(partition) is None:
        # Without a quoted date, a bare integer is the partition ID itself
        # (for example "202402") rather than a separate key component.
        return None

    matches = _PARTITION_INTEGER.findall(partition)
    if len(matches) != 1:
        return None
    return int(matches[0])


def get_partition_boundaries(
    clickhouse: ClickhousePool, database: str, table: str
) -> dict[str, str]:
    """
    Map each active partition ID of a table to its ``partition`` value.

    The ``partition`` value carries the readable partition key, which is the
    only way to recover a time boundary when the partition ID is hashed.
    """
    response = clickhouse.execute(
        """
        SELECT DISTINCT partition_id, partition
        FROM system.parts
        WHERE database = %(database)s
        AND table = %(table)s
        AND active = 1
        """,
        {"database": database, "table": table},
    )
    return dict(response.results)


def run_health_check_query(
    clickhouse: ClickhousePool,
    query: str,
    partition_id: str,
    partition: str | None = None,
) -> None:
    """
    Run an operator-supplied health check scoped to one partition.

    The query may reference ``%(partition_start)s``, bound to the start of the
    partition's time boundary, and ``%(retention_days)s`` when the partition key
    carries one. Both are needed to identify a single partition of a table
    partitioned by ``(retention_days, toMonday(timestamp))``, because partitions
    that differ only by retention share a boundary.

    The destination is considered unhealthy when the query raises, returns no
    rows, or returns a falsy first value.
    """
    partition_value = partition if partition is not None else partition_id
    partition_start = parse_partition_start(partition_value)
    retention_days = parse_retention_days(partition_value)

    parameters: dict[str, object] = {PARTITION_START_PARAM: partition_start}
    if retention_days is not None:
        parameters[RETENTION_DAYS_PARAM] = retention_days

    missing = [name for name in _referenced_parameters(query) if name not in parameters]
    if missing:
        available = ", ".join(sorted(parameters))
        raise PartitionBoundaryError(
            f"Health check query references unavailable parameter(s) "
            f"{', '.join(sorted(missing))} for partition {partition_id}. "
            f"Available parameter(s): {available}."
        )

    response = clickhouse.execute(query, parameters)

    results = response.results
    if not results or not results[0] or not results[0][0]:
        scope = f"partition start {partition_start.isoformat()}"
        if retention_days is not None:
            scope += f", retention_days {retention_days}"
        raise UnhealthyError(
            f"Health check query reported unhealthy before partition {partition_id} ({scope})"
        )


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


def build_health_check(
    clickhouse: ClickhousePool,
    health_check_query: str | None,
    boundaries: dict[str, str] | None = None,
) -> HealthCheck:
    """
    Build the per-partition health check.

    Without a query this falls back to a minimal connectivity check that ignores
    the partition being attached. ``boundaries`` maps partition IDs to their
    readable ``partition`` values, which is required for tables whose partition
    IDs are hashed.
    """
    if health_check_query is None:
        return lambda partition_id: check_clickhouse_health(clickhouse)

    return lambda partition_id: run_health_check_query(
        clickhouse,
        health_check_query,
        partition_id,
        (boundaries or {}).get(partition_id),
    )


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
    health_check_query: str | None = None,
    on_partition_attached: PartitionAttached | None = None,
    dry_run: bool = False,
) -> Sequence[str]:
    """
    Attach active source partitions that are not active on the destination.

    Partitions are attached serially. The health check runs before every attach,
    which gates the first operation and every operation following an attach. It
    receives the partition ID that is about to be attached, so it can scope
    itself to that partition's time boundary. A failed check aborts the process
    without attempting the next partition.

    Passing ``health_check_query`` builds that scoped check here, resolving each
    partition's boundary from the source table so it works even when partition
    IDs are hashed. An explicit ``health_check`` takes precedence.

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

    if health_check is not None:
        check_health = health_check
    else:
        boundaries = (
            get_partition_boundaries(clickhouse, database, source_table)
            if health_check_query is not None
            else None
        )
        check_health = build_health_check(clickhouse, health_check_query, boundaries)

    attached_partition_ids = []
    for partition_id in pending_partition_ids:
        check_health(partition_id)
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
