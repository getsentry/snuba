import time
from datetime import datetime, timezone
from typing import Optional

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import Message

from snuba.state import get_int_config
from snuba.utils.metrics import MetricsBackend

_CACHE_TTL_SECONDS = 60


class OffPeakProcessingStrategy(ProcessingStrategy[KafkaPayload]):
    """
    Wraps another ProcessingStrategy and only forwards messages during
    configurable off-peak hours. Outside those hours, raises MessageRejected
    to apply backpressure so messages accumulate in Kafka.

    Controlled via runtime config (Redis-backed):
      - lw_deletions_offpeak_enabled (int, default 0): feature toggle
      - lw_deletions_offpeak_start (int, default 0): start hour UTC, inclusive
      - lw_deletions_offpeak_end (int, default 24): end hour UTC, exclusive
    """

    def __init__(
        self,
        next_step: ProcessingStrategy[KafkaPayload],
        metrics: MetricsBackend,
    ) -> None:
        self.__next_step = next_step
        self.__metrics = metrics
        self.__cached_result: Optional[bool] = None
        self.__cached_at: float = 0.0

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        if not self._is_off_peak():
            self.__metrics.increment("off_peak_rejected")
            raise MessageRejected
        self.__next_step.submit(message)

    def _is_off_peak(self) -> bool:
        now = time.time()
        if self.__cached_result is not None and (now - self.__cached_at) < _CACHE_TTL_SECONDS:
            return self.__cached_result

        enabled = get_int_config("lw_deletions_offpeak_enabled", default=0)
        if not enabled:
            self.__cached_result = True
            self.__cached_at = now
            return True

        start = get_int_config("lw_deletions_offpeak_start", default=0) or 0
        end = get_int_config("lw_deletions_offpeak_end", default=24) or 24
        current_hour = datetime.now(timezone.utc).hour

        if start == end:
            result = False
        elif start < end:
            result = start <= current_hour < end
        else:
            # Window spans midnight (e.g., 22-6)
            result = current_hour >= start or current_hour < end

        self.__cached_result = result
        self.__cached_at = now
        return result

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)
