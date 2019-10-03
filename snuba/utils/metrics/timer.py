import time
from itertools import groupby
from typing import Optional, Mapping, MutableSequence, Tuple
from mypy_extensions import TypedDict

from snuba.utils.metrics import Metrics


class TimerData(TypedDict):
    timestamp: int
    duration_ms: int
    marks_ms: Mapping[str, int]


class Timer:
    def __init__(self, name: str):
        self.__name = name
        self.__marks: MutableSequence[Tuple[str, float]] = [(name, time.time())]
        self.__data: Optional[TimerData] = None

    def mark(self, name: str) -> None:
        self.__data = None
        self.__marks.append((name, time.time()))

    def __diff_ms(self, start: float, end: float) -> int:
        return int((end - start) * 1000)

    def finish(self) -> TimerData:
        if self.__data is None:
            start = self.__marks[0][1]
            end = time.time() if len(self.__marks) == 1 else self.__marks[-1][1]
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
        metrics: Metrics,
        tags: Optional[Mapping[str, str]] = None,
        mark_tags: Optional[Mapping[str, str]] = None,
    ) -> None:
        data = self.finish()
        metrics.timing(self.__name, data["duration_ms"], tags=tags)
        for mark, duration in data["marks_ms"].items():
            metrics.timing(f"{self.__name}.{mark}", duration, tags=mark_tags)
