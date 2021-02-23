from itertools import groupby
from typing import Optional, Mapping, MutableSequence, Tuple, TYPE_CHECKING

from snuba.utils.clock import Clock, SystemClock
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags

if TYPE_CHECKING:
    from mypy_extensions import TypedDict

    class TimerData(TypedDict):
        timestamp: int
        duration_ms: int
        marks_ms: Mapping[str, int]


else:
    from typing import Any

    TimerData = Mapping[Any, Any]


class Timer:
    def __init__(
        self, name: str, clock: Clock = SystemClock(), init_time: Optional[float] = None
    ):
        self.__name = name
        self.__clock = clock

        self.__marks: MutableSequence[Tuple[str, float]] = [
            (self.__name, self.__clock.time() if init_time is None else init_time)
        ]
        self.__data: Optional[TimerData] = None

    def mark(self, name: str, timestamp: Optional[float] = None) -> None:
        self.__data = None
        self.__marks.append(
            (name, self.__clock.time() if timestamp is None else timestamp)
        )

    def __diff_ms(self, start: float, end: float) -> int:
        return int((end - start) * 1000)

    def finish(self) -> TimerData:
        if self.__data is None:
            start = self.__marks[0][1]
            end = self.__clock.time() if len(self.__marks) == 1 else self.__marks[-1][1]
            durations = [
                (name, self.__diff_ms(self.__marks[i][1], ts))
                for i, (name, ts) in enumerate(self.__marks[1:])
            ]
            self.__data = {
                "timestamp": int(start),
                "duration_ms": self.__diff_ms(start, end),
                "marks_ms": {
                    key: sum(d[1] for d in group)
                    for key, group in groupby(sorted(durations), key=lambda x: x[0])
                },
            }
        return self.__data

    def for_json(self) -> TimerData:
        return self.finish()

    def send_metrics_to(
        self,
        backend: MetricsBackend,
        tags: Optional[Tags] = None,
        mark_tags: Optional[Tags] = None,
    ) -> None:
        data = self.finish()
        backend.timing(self.__name, data["duration_ms"], tags=tags)
        for mark, duration in data["marks_ms"].items():
            backend.timing(f"{self.__name}.{mark}", duration, tags=mark_tags)
