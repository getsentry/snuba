from snuba.manual_jobs import Job, JobLogger, JobSpec


class ToyJob(Job):
    def __init__(
        self,
        job_spec: JobSpec,
    ):
        super().__init__(job_spec)

    def _build_query(self) -> str:
        return "query"

    def execute(self, logger: JobLogger) -> None:
        logger.info(
            "executing job " + self.job_spec.job_id + " with query `" + self._build_query() + "`"
        )

        if not self.job_spec.params:
            return

        if self.job_spec.params.get("fail"):
            raise Exception("failed as requested")
