from logging import CRITICAL, DEBUG, ERROR, INFO, NOTSET, WARNING

PYTHON_TO_SYSLOG_MAP = {
    NOTSET: 7,
    DEBUG: 7,
    INFO: 6,
    WARNING: 4,
    ERROR: 3,
    CRITICAL: 2,
}


def pylog_to_syslog_level(level: int) -> int:
    return PYTHON_TO_SYSLOG_MAP.get(level, 7)
