from typing import Any, cast

from snuba.manual_jobs import Job


class JobLoader:
    @staticmethod
    def get_job_instance(class_name: str, dry_run: bool, **kwargs: Any) -> "Job":
        job_type_class = Job.class_from_name(class_name)
        if job_type_class is None:
            raise Exception(
                f"Job does not exist. Did you make a file {class_name}.py yet?"
            )

        return cast("Job", job_type_class(dry_run=dry_run, **kwargs))
