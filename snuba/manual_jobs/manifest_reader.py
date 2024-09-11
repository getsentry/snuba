import os
from typing import Mapping

import simplejson

from snuba.manual_jobs import JobSpec


class _ManifestReader:
    @staticmethod
    def read(filename: str) -> Mapping[str, JobSpec]:
        local_root = os.path.dirname(__file__)

        with open(os.path.join(local_root, filename)) as stream:
            contents = simplejson.loads(stream.read())

            job_specs = {}
            for content in contents:

                job_id = content["id"]
                assert isinstance(job_id, str)
                job_type = content["job_type"]
                assert isinstance(job_type, str)

                job_spec = JobSpec(
                    job_id=job_id,
                    job_type=job_type,
                    params=content.get("params"),
                )
                job_specs[job_id] = job_spec
            return job_specs


def read_jobs_manifest() -> Mapping[str, JobSpec]:
    return _ManifestReader.read("job_manifest.json")
