from logging import Logger
from time import asctime

from snuba.manual_jobs import JobLogger
from snuba.manual_jobs.redis import _push_job_log_line

_REDIS_LOG_FORMAT = "[%(levelname)s] %(asctime)s: %(message)s"


def get_job_logger(logger: Logger, job_id: str) -> JobLogger:
    return _MultiplexingRedisLogger(logger, job_id)


class _MultiplexingRedisLogger(JobLogger):
    def __init__(self, logger: Logger, job_id: str):
        self.logger = logger
        self.job_id = job_id

    def _make_redis_log_line(self, line: str, level: str) -> str:
        return _REDIS_LOG_FORMAT % {
            "asctime": asctime(),
            "levelname": level,
            "message": line,
        }

    def debug(self, line: str) -> None:
        self.logger.debug(line)
        _push_job_log_line(self.job_id, line=self._make_redis_log_line(line, level="DEBUG"))

    def info(self, line: str) -> None:
        self.logger.info(line)
        _push_job_log_line(self.job_id, line=self._make_redis_log_line(line, level="INFO"))

    def warning(self, line: str) -> None:
        self.logger.warning(line)
        _push_job_log_line(self.job_id, line=self._make_redis_log_line(line, level="WARNING"))

    def warn(self, line: str) -> None:
        self.warning(line)

    def error(self, line: str) -> None:
        self.logger.error(line)
        _push_job_log_line(self.job_id, line=self._make_redis_log_line(line, level="ERROR"))
