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

import logging
import click
import sys
from typing import Optional

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES
from snuba.util import local_dataset_mode


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
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
def perf(
    *,
    events_file: Optional[str],
    repeat: int,
    profile_process: bool,
    profile_write: bool,
    dataset_name: str,
    log_level: str,
) -> None:
    from snuba.perf import run, logger

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format="%(asctime)s %(message)s"
    )

    dataset = get_dataset(dataset_name)
    if not local_dataset_mode():
        logger.error("The perf tool is only intended for local dataset environment.")
        sys.exit(1)

    run(
        events_file,
        dataset,
        repeat=repeat,
        profile_process=profile_process,
        profile_write=profile_write,
    )
