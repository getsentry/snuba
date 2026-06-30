from dataclasses import dataclass
from typing import cast

from snuba.state.sentry_options import get_option

_OPTIMIZE_PARALLEL_THREADS_KEY = "optimize_parallel_threads"


@dataclass
class MergeInfo:
    result_part_name: str
    elapsed: float
    progress: float
    size: int

    @property
    # estimated time remaining in seconds
    def estimated_time(self) -> float:
        return self.elapsed / (self.progress + 0.0001)


def get_num_threads(default_parallel_threads: int) -> int:
    # A 0 (the schema default) means "unset": fall back to the value passed via
    # the optimize command's --parallel flag, preserving the prior runtime-config
    # behavior where the option was only an override.
    return cast(int, get_option(_OPTIMIZE_PARALLEL_THREADS_KEY, 0)) or default_parallel_threads
