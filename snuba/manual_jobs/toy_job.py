from snuba.manual_jobs import Job, JobSpec, logger


class ToyJob(Job):
    def __init__(
        self,
        job_spec: JobSpec,
        dry_run: bool,
    ):
        super().__init__(job_spec, dry_run)

    def _build_query(self) -> str:
        if self.dry_run:
            return "dry run query"
        else:
            return "not dry run query"

    def execute(self) -> None:
        logger.info(
            "executing job "
            + self.job_spec.job_id
            + " with query `"
            + self._build_query()
            + "`"
        )
