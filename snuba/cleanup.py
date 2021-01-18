import logging
from datetime import datetime, timedelta
from typing import Optional, Sequence

from snuba import util
from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import WritableTableStorage

logger = logging.getLogger("snuba.cleanup")


def run_cleanup(
    clickhouse: ClickhousePool,
    storage: WritableTableStorage,
    database: str,
    dry_run: bool = True,
) -> int:

    table = storage.get_table_writer().get_schema().get_local_table_name()

    active_parts = get_active_partitions(clickhouse, storage, database, table)
    stale_parts = filter_stale_partitions(active_parts)
    drop_partitions(clickhouse, database, table, stale_parts, dry_run=dry_run)
    return len(stale_parts)


def get_active_partitions(
    clickhouse: ClickhousePool, storage: WritableTableStorage, database: str, table: str
) -> Sequence[util.Part]:
    response = clickhouse.execute(
        """
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = %(database)s
        AND table = %(table)s
        AND active = 1
        """,
        {"database": database, "table": table},
    )

    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    part_format = schema.get_part_format()
    assert part_format is not None
    return [util.decode_part_str(part, part_format) for part, in response]


def filter_stale_partitions(
    parts: Sequence[util.Part], as_of: Optional[datetime] = None
) -> Sequence[util.Part]:
    """Filter partitions down to ones that are out of the retention
    window based on `as_of` (default: now)."""

    if as_of is None:
        as_of = datetime.utcnow()

    stale_parts = []
    for part in parts:
        part_date = part.date
        retention_days = part.retention_days
        part_last_day = part_date + timedelta(days=6 - part_date.weekday())

        if part_last_day < (as_of - timedelta(days=retention_days)):
            stale_parts.append(part)
    return stale_parts


def drop_partitions(
    clickhouse: ClickhousePool,
    database: str,
    table: str,
    parts: Sequence[util.Part],
    dry_run: bool = True,
) -> None:
    query_template = """\
        ALTER TABLE %(database)s.%(table)s DROP PARTITION %(partition)s
    """

    for part in parts:
        args = {
            "database": database,
            "table": table,
            "partition": part.name,
        }

        query = (query_template % args).strip()
        if dry_run:
            logger.info("Dry run: " + query)
        else:
            logger.info("Dropping partition: " + query)
            clickhouse.execute(query)
