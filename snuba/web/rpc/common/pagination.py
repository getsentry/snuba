"""
This file contains constants that are used to encode pagination details into
a filter_offset page token
"""


class FlexibleTimeWindow:
    START_TIMESTAMP_KEY = "sentry.start_timestamp"
    END_TIMESTAMP_KEY = "sentry.end_timestamp"
    OFFSET_KEY = "sentry.offset"
