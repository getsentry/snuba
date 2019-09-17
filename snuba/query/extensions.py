from datetime import timedelta
from typing import Any, Mapping, Tuple

from snuba import state
from snuba import util

PERFORMANCE_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        # Never add FINAL to queries, enable sampling
        'turbo': {
            'type': 'boolean',
            'default': False,
        },
        # Force queries to hit the first shard replica, ensuring the query
        # sees data that was written before the query. This burdens the
        # first replica, so should only be used when absolutely necessary.
        'consistent': {
            'type': 'boolean',
            'default': False,
        },
        'debug': {
            'type': 'boolean',
            'default': False,
        },
    },
    'additionalProperties': False,
}

PROJECT_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        'project': {
            'anyOf': [
                {'type': 'integer', 'minimum': 1},
                {
                    'type': 'array',
                    'items': {'type': 'integer', 'minimum': 1},
                    'minItems': 1,
                },
            ]
        },
    },
    # Need to select down to the project level for customer isolation and performance
    'required': ['project'],
    'additionalProperties': False,
}


def get_time_limit(timeseries_extension: Mapping[str, Any]) -> Tuple[int, int]:
    max_days, date_align = state.get_configs([
        ('max_days', None),
        ('date_align_seconds', 1),
    ])

    to_date = util.parse_datetime(timeseries_extension['to_date'], date_align)
    from_date = util.parse_datetime(timeseries_extension['from_date'], date_align)
    assert from_date <= to_date

    if max_days is not None and (to_date - from_date).days > max_days:
        from_date = to_date - timedelta(days=max_days)

    return (from_date, to_date)
