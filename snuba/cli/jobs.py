from typing import Any, MutableMapping, Tuple

import click

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.runner import (
    MANIFEST_FILENAME,
    get_job_status,
    list_job_specs,
    run_job,
)

JOB_SPECIFICATION_ERROR_MSG = "Missing job type and/or job id"


@click.group()
def jobs() -> None:
    pass


@jobs.command()
@click.option("--json_manifest", default=MANIFEST_FILENAME)
def list(*, json_manifest: str) -> None:
    job_specs = list_job_specs(json_manifest)
    click.echo(job_specs)


def _run_job_and_echo_status(job_spec: JobSpec, dry_run: bool) -> None:
    status = run_job(job_spec, dry_run)
    click.echo(f"resulting job status = {status}")


@jobs.command()
@click.option("--json_manifest", default=MANIFEST_FILENAME)
@click.option("--job_id")
@click.option(
    "--dry_run",
    default=True,
)
def run_from_manifest(*, json_manifest: str, job_id: str, dry_run: bool) -> None:
    job_specs = list_job_specs(json_manifest)
    if job_id not in job_specs.keys():
        raise click.ClickException("Provide a valid job id")

    _run_job_and_echo_status(job_specs[job_id], dry_run)


def _parse_params(pairs: Tuple[str, ...]) -> MutableMapping[Any, Any]:
    return {k: v for k, v in (pair.split("=") for pair in pairs)}


@jobs.command()
@click.option("--job_type")
@click.option("--job_id")
@click.option(
    "--dry_run",
    default=True,
)
@click.argument("pairs", nargs=-1)
def run(*, job_type: str, job_id: str, dry_run: bool, pairs: Tuple[str, ...]) -> None:
    if not job_type or not job_id:
        raise click.ClickException(JOB_SPECIFICATION_ERROR_MSG)
    job_spec = JobSpec(job_id=job_id, job_type=job_type, params=_parse_params(pairs))

    _run_job_and_echo_status(job_spec, dry_run)


@jobs.command()
@click.option("--job_id")
def status(*, job_id: str) -> None:
    click.echo(get_job_status(job_id))
