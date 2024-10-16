from enum import StrEnum


class JobStatus(StrEnum):
    RUNNING = "running"
    FINISHED = "finished"
    NOT_STARTED = "not_started"
    ASYNC_RUNNING_BACKGROUND = "async_running_background"
    FAILED = "failed"
