from logging import Logger


class _MultiplexingRedisLogger:
    def __init__(self, *loggers: Logger):
        self.loggers = loggers

    def log(self, *args, **kwargs):
        for logger in self.loggers:
            logger.log(*args, **kwargs)
