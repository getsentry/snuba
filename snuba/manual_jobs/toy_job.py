import logging

from snuba.manual_jobs import Job, JobSpec


class ToyJob(Job):
    def __init__(
        self,
        job_spec: JobSpec,
    ):
        super().__init__(job_spec)

    def _build_query(self) -> str:
        return "query"

    def execute(self, logger: logging.Logger) -> None:
        logger.info(
            "executing job "
            + self.job_spec.job_id
            + " with query `"
            + self._build_query()
            + "`"
        )
