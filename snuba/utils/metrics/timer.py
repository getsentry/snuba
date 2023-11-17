from itertools import groupby
from typing import Dict, Mapping, MutableSequence, Optional, Tuple

from sentry_kafka_schemas.schema_types.snuba_queries_v1 import TimerData

from snuba.utils.clock import Clock, SystemClock
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class Timer:
    def __init__(
        self,
        name: str,
        clock: Clock = SystemClock(),
        tags: Optional[Mapping[str, str]] = None,
    ):
        self.__name = name
        self.__clock = clock

        self.__marks: MutableSequence[Tuple[str, float]] = [
            (self.__name, self.__clock.time())
        ]
        self.__data: Optional[TimerData] = None
        self.__tags: Dict[str, str] = dict(tags or {})

    def mark(self, name: str) -> None:
        self.__data = None
        self.__marks.append((name, self.__clock.time()))

    def __diff_ms(self, start: float, end: float) -> int:
        return int((end - start) * 1000)

    def get_duration_group(self) -> str:
        if self.__data is None:
            return "unknown"

        duration_ms = self.__data["duration_ms"]
        duration_group = "<10s"
        if duration_ms >= 30000:
            duration_group = ">30s"
        elif duration_ms >= 20000:
            duration_group = ">20s"
        elif duration_ms >= 10000:
            duration_group = ">10s"

        return duration_group

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
                "tags": self.__tags,
            }

        return self.__data

    @property
    def tags(self) -> Optional[Mapping[str, str]]:
        return self.__tags

    def for_json(self) -> TimerData:
        return self.finish()

    def send_metrics_to(
        self,
        backend: MetricsBackend,
        tags: Optional[Tags] = None,
        mark_tags: Optional[Tags] = None,
    ) -> None:
        data = self.finish()
        merged_tags = {**data["tags"], **tags} if tags else self.__tags
        backend.timing(self.__name, data["duration_ms"], tags=merged_tags)
        for mark, duration in data["marks_ms"].items():
            merged_mark_tags = (
                {**data["tags"], **mark_tags} if mark_tags else data["tags"]
            )
            backend.timing(f"{self.__name}.{mark}", duration, tags=merged_mark_tags)
