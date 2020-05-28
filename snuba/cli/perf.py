"""\
Snuba "perf" is a small script to help track the consumer (inserter) performance.

snuba perf --events-file tests/perf-event.json --repeat 5000
Number of events:       5000
Total:               3835.98ms
Total process:       1815.71ms
Total write:         2020.27ms
Process event:          0.36ms/ea
Writer event:           0.36ms/ea

The `events-file` should be newline delimited JSON events, as seen from the
main `events` topic. Retrieving a sample file is left as an excercise for the
reader, or you can use the `tests/perf-event.json` file with a high repeat count.
"""

import click
import sys
from typing import Optional

from snuba.datasets.factory import get_dataset, DATASET_NAMES
from snuba.environment import setup_logging


@click.command()
@click.option("--events-file", help="Event JSON input file.")
@click.option(
    "--repeat", type=int, default=1, help="Number of times to repeat the input."
)
@click.option(
    "--profile-process/--no-profile-process",
    default=False,
    help="Whether or not to profile processing.",
)
@click.option(
    "--profile-write/--no-profile-write",
    default=False,
    help="Whether or not to profile writing.",
)
@click.option(
    "--dataset",
    "dataset_name",
    default="events",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to consume/run replacements for (currently only events supported)",
)
@click.option("--log-level", help="Logging level to use.")
def perf(
    *,
    events_file: Optional[str],
    repeat: int,
    profile_process: bool,
    profile_write: bool,
    dataset_name: str,
    log_level: Optional[str] = None,
) -> None:
    from snuba.perf import run, logger

    setup_logging(log_level)

    dataset = get_dataset(dataset_name)

    if not all(
        [
            storage.get_cluster().is_single_node()
            for storage in dataset.get_all_storages()
        ]
    ):
        logger.error("The perf tool is only intended for single node environment.")
        sys.exit(1)

    run(
        events_file,
        dataset,
        repeat=repeat,
        profile_process=profile_process,
        profile_write=profile_write,
    )
