import csv
from typing import Optional

import click
import structlog
import itertools
from dataclasses import dataclass

from snuba.environment import setup_logging, setup_sentry

logger = structlog.get_logger().bind(module=__name__)


@dataclass
class QueryMeasurement:
    query_id: str
    query_duration_ms: int
    result_rows: int
    result_bytes: int


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
            query_duration_ms=row[1],
            result_rows=row[2],
            result_bytes=row[3],
        )

    def compare_query(v1_data, v2_data):
        # TODO() compare the results from each time
        # we ran the query and decide what to do with
        # the mismatched results

        pass

    for v1_row, v2_row in itertools.zip_longest(base_reader, upgrade_reader):
        if v1_row[0] == "query_id":
            # csv header row
            continue
        v1_data = query_measurement(v1_row)
        v2_data = query_measurement(v2_row)

        assert v1_data.query_id == v2_data.query_id

        compare_query(v1_data, v2_data)
