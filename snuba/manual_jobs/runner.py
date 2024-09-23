import os
from typing import Any, Mapping

import simplejson

from snuba.manual_jobs import JobSpec


def _read_from_path(filename: str) -> Mapping[str, JobSpec]:
    local_root = os.path.dirname(__file__)

    with open(os.path.join(local_root, filename)) as stream:
        contents = simplejson.loads(stream.read())

        job_specs = {}
        for content in contents:
            job_spec = _build_job_spec_from_entry(content)
            job_specs[job_spec.job_id] = job_spec
        return job_specs


def _build_job_spec_from_entry(content: Any) -> JobSpec:
    job_id = content["id"]
    assert isinstance(job_id, str)
    job_type = content["job_type"]
    assert isinstance(job_type, str)

    job_spec = JobSpec(
        job_id=job_id,
        job_type=job_type,
        params=content.get("params"),
    )

    return job_spec


def list_job_specs(
    jobs_filename: str = "job_manifest.json",
) -> Mapping[str, JobSpec]:
    return _read_from_path(jobs_filename)
