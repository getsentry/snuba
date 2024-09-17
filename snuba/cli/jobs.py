from typing import Any, MutableMapping, Tuple

import click

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_loader import JobLoader
from snuba.manual_jobs.manifest_reader import _ManifestReader

JOB_SPECIFICATION_ERROR_MSG = "Missing job type and/or job id"


@click.group()
def jobs() -> None:
    pass


@jobs.command()
@click.option("--json_manifest", required=True)
@click.option("--job_id")
@click.option(
    "--dry_run",
    default=True,
)
def run_from_manifest(*, json_manifest: str, job_id: str, dry_run: bool) -> None:
    job_specs = _ManifestReader.read(json_manifest)
    if job_id not in job_specs.keys():
        raise click.ClickException("Provide a valid job id")

    job_to_run = JobLoader.get_job_instance(job_specs[job_id], dry_run)
    job_to_run.execute()


def _parse_params(pairs: Tuple[str, ...]) -> MutableMapping[Any, Any]:
    params = {}
    for pair in pairs:
        k, v = pair.split("=")
        params[k] = v
    return params


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

    job_to_run = JobLoader.get_job_instance(job_spec, dry_run)
    job_to_run.execute()
