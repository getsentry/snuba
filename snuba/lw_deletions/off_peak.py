import time
from datetime import UTC, datetime

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import Message

from snuba.state.sentry_options import get_bool_option, get_int_option
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
        self.__cached_result: bool | None = None
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

        enabled = get_bool_option("lw_deletions_offpeak_enabled", False)
        if not enabled:
            self.__cached_result = True
            self.__cached_at = now
            return True

        start = get_int_option("lw_deletions_offpeak_start", 0)
        end = get_int_option("lw_deletions_offpeak_end", 24)
        current_hour = datetime.now(UTC).hour

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

    def join(self, timeout: float | None = None) -> None:
        self.__next_step.join(timeout)
