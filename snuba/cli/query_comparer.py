import csv
from typing import Optional

import click
import structlog
import itertools
from dataclasses import dataclass

from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)


@dataclass(frozen=True)
class QueryMeasurement:
    query_id: str
    query_duration_ms: int
    result_rows: int
    result_bytes: int
    read_rows: int
    read_bytes: int

@dataclass(frozen=True)
class MismatchedValues:
    base_value: int
    new_value: int
    delta: int
    delta_percent: float

@dataclass
class QueryMismatch:
    query_id: str
    query_duration_ms: MismatchedValues
    result_rows: MismatchedValues
    result_bytes: MismatchedValues
    read_rows: MismatchedValues
    read_bytes: MismatchedValues

MEASUREMENTS = [
    "query_duration_ms",
    "result_rows",
    "result_bytes",
    "read_rows",
    "read_bytes",
]


@click.command()
@click.option(
    "--base-file",
    help="Clickhouse queries results from base version.",
    required=True,
)
@click.option(
    "--upgrade-file",
    help="Clickhouse queries results from upgrade version.",
    required=True,
)
@click.option("--log-level", help="Logging level to use.")
def query_comparer(
    *,
    base_file: str,
    upgrade_file: str,
    log_level: Optional[str] = None,
) -> None:
    """
    compare results
    """
    setup_logging(log_level)
    setup_sentry()

    base = open(base_file)
    upgrade = open(upgrade_file)

    base_reader = csv.reader(base)
    upgrade_reader = csv.reader(upgrade)

    def query_measurement(row) -> QueryMeasurement:
        return QueryMeasurement(
            query_id=row[0],
            query_duration_ms=int(row[1]),
            result_rows=int(row[2]),
            result_bytes=int(row[3]),
            read_rows=int(row[4]),
            read_bytes=int(row[5]),
        )

    mismatches = []
    total_rows = 0
    for v1_row, v2_row in itertools.zip_longest(base_reader, upgrade_reader):
        if v1_row[0] == "query_id":
            # csv header row
            continue

        assert v1_row[0] == v2_row[0], "Invalid query_ids: ids must match"
        total_rows += 1

        mismatches_exist = any([1 for i in range(1, len(v1_row)) if v1_row[i] != v2_row[i]])
        if not mismatches_exist:
            continue

        v1_data = query_measurement(v1_row)
        v2_data = query_measurement(v2_row)

        mismatches.append(_create_mismatch(v1_data, v2_data))

    print("\ntotal queries:", total_rows)
    print("total mismatches:", len(mismatches))
    print("percent mismatches:", len(mismatches)/total_rows)


def _create_mismatch(v1_data, v2_data):
    mismatch = {}
    for m in MEASUREMENTS:
        v1_value = v1_data.__getattribute__(m)
        v2_value = v2_data.__getattribute__(m)

        delta = v2_value - v1_value
        delta_percent = (delta/v1_value) * 100
        mismatched_values = MismatchedValues(
            v1_value,
            v2_value,
            delta,
            delta_percent,
        )
        mismatch[m] = mismatched_values
    return QueryMismatch(
        query_id=v1_data.query_id,
        query_duration_ms=mismatch["query_duration_ms"],
        result_rows=mismatch["result_rows"],
        result_bytes=mismatch["result_bytes"],
        read_rows=mismatch["read_rows"],
        read_bytes=mismatch["read_bytes"],
    )
