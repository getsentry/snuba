from typing import cast

from snuba.manual_jobs import Job, JobSpec
from snuba.utils.serializable_exception import SerializableException


class NonexistentJobException(SerializableException):
    def __init__(self, job_type: str):
        super().__init__(f"Job does not exist. Did you make a file {job_type}.py yet?")


class _JobLoader:
    @staticmethod
    def get_job_instance(job_spec: JobSpec) -> "Job":
        job_type_class = Job.class_from_name(job_spec.job_type)
        if job_type_class is None:
            raise NonexistentJobException(job_spec.job_type)

        return cast("Job", job_type_class(job_spec))
