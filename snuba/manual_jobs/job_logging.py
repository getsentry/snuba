from logging import Logger
from time import asctime

from snuba.manual_jobs.redis import _push_job_log_line

_REDIS_LOG_FORMAT = "[%(levelname)s] %(asctime)s: %(message)s"


class _MultiplexingRedisLogger:
    def __init__(self, logger: Logger, job_id: str):
        self.logger = logger
        self.job_id = job_id

    def _make_redis_log_line(self, line: str, level: str) -> str:
        return _REDIS_LOG_FORMAT % {
            "asctime": asctime(),
            "levelname": level,
            "message": line,
        }

    def debug(self, line: str, **kwargs):
        self.logger.debug(line, **kwargs)
        _push_job_log_line(
            self.job_id, line=self._make_redis_log_line(line % kwargs, level="DEBUG")
        )

    def info(self, line: str, **kwargs):
        self.logger.info(line, **kwargs)
        _push_job_log_line(
            self.job_id, line=self._make_redis_log_line(line % kwargs, level="INFO")
        )

    def warning(self, line: str, **kwargs):
        self.logger.warning(line, **kwargs)
        _push_job_log_line(
            self.job_id, line=self._make_redis_log_line(line % kwargs, level="WARNING")
        )

    def warn(self, line: str, **kwargs):
        self.warn(line, **kwargs)

    def error(self, line: str, **kwargs):
        self.logger.error(line, **kwargs)
        _push_job_log_line(
            self.job_id, line=self._make_redis_log_line(line % kwargs, level="ERROR")
        )
