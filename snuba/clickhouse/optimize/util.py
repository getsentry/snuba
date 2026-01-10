import typing
from dataclasses import dataclass

from snuba.state import get_config

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
    return typing.cast(int, get_config(_OPTIMIZE_PARALLEL_THREADS_KEY, default_parallel_threads))
