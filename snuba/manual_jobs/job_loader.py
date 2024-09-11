from typing import cast

from snuba.manual_jobs import Job, JobSpec


class JobLoader:
    @staticmethod
    def get_job_instance(job_spec: JobSpec, dry_run: bool) -> "Job":
        job_type_class = Job.class_from_name(job_spec.job_type)
        if job_type_class is None:
            raise Exception(
                f"Job does not exist. Did you make a file {job_spec.job_type}.py yet?"
            )

        return cast("Job", job_type_class(job_spec, dry_run=dry_run))
