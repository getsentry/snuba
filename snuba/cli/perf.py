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

from snuba import settings


@click.command()
@click.option('--events-file', help='Event JSON input file.')
@click.option('--repeat', default=1, help='Number of times to repeat the input.')
@click.option(
    '--profile-process/--no-profile-process',
    default=False,
    help='Whether or not to profile processing.',
)
@click.option(
    '--profile-write/--no-profile-write',
    default=False,
    help='Whether or not to profile writing.',
)
@click.option(
    '--clickhouse-server',
    default=settings.CLICKHOUSE_SERVER,
    help='Clickhouse server to run perf against.',
)
@click.option(
    '--dataset',
    default='events',
    type=click.Choice(['events']),
    help='The dataset to consume/run replacements for (currently only events supported)',
)
@click.option('--log-level', default=settings.LOG_LEVEL, help='Logging level to use.')
def perf(
    events_file,
    repeat,
    profile_process,
    profile_write,
    clickhouse_server,
    dataset,
    log_level,
):
    from snuba.clickhouse import ClickhousePool
    from snuba.perf import run, logger

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format='%(asctime)s %(message)s'
    )

    if settings.CLICKHOUSE_TABLE != 'dev':
        logger.error(
            "The migration tool is only intended for local development environment."
        )
        sys.exit(1)

    dataset = settings.get_dataset(dataset)
    clickhouse = ClickhousePool(
        clickhouse_server.split(':')[0], port=int(clickhouse_server.split(':')[1])
    )
    run(
        events_file,
        clickhouse,
        dataset,
        repeat=repeat,
        profile_process=profile_process,
        profile_write=profile_write,
    )
