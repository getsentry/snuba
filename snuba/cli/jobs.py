from typing import Tuple

import click

from snuba.manual_jobs.job_loader import JobLoader


@click.group()
def jobs() -> None:
    pass


@jobs.command()
@click.argument("job_name")
@click.option(
    "--dry_run",
    default=True,
)
@click.argument("pairs", nargs=-1)
def run(*, job_name: str, dry_run: bool, pairs: Tuple[str, ...]) -> None:

    kwargs = {}
    for pair in pairs:
        k, v = pair.split("=")
        kwargs[k] = v

    job_to_run = JobLoader.get_job_instance(job_name, dry_run, **kwargs)
    job_to_run.execute()
