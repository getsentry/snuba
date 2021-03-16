# Python log levels are:
# CRITICAL 50
# ERROR    40
# WARNING  30
# INFO     20
# DEBUG    10
# NOTSET    0
# syslog(3) levels are:
# EMERGENCY 0
# ALERT     1
# CRITICAL  2
# ERROR     3
# WARNING   4
# NOTICE    5
# INFO      6
# DEBUG     7


def pylog_to_syslog_level(level: int) -> int:
    if level == 0:  # NOTSET
        return 7  # DEBUG, to process all messages

    syslog_level = 8 - level // 10
    if syslog_level <= 5:
        syslog_level -= 1  # Adjust for lack of NOTICE level on Python

    return syslog_level
