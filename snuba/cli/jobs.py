import click

from snuba.manual_jobs.ToyJob import ToyJob


@click.command()
@click.option(
    "--dry_run",
    is_flag=True,
    default=True,
)
@click.option("--storage_name")
def jobs(*, dry_run: bool, storage_name: str) -> None:
    job_to_run = ToyJob(dry_run, storage_name)
    job_to_run.execute()
