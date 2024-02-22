from __future__ import annotations

import sys

import click

from snuba.utils.health_info import get_health_info


@click.command()
@click.option(
    "--thorough",
    help="Whether to run a thorough health check.",
    is_flag=True,
    default=False,
)
def health(
    *,
    thorough: bool,
) -> int:

    _, health_status, _ = get_health_info(thorough)
    if health_status == 200:
        sys.exit(0)
    else:
        sys.exit(1)
