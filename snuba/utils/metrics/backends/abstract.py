from abc import ABC, abstractmethod
from typing import Optional, Union

from snuba.utils.metrics.types import Tags


class MetricsBackend(ABC):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        """
        Increment a counter metric. These increments can also be
        sliced/diced into percentiles, or the sum read over a window. For "decrement",
        use a negative value.

        Examples:

        metrics.increment("messages_procesed", finished_message_count)
        metrics.increment("net_account_deposits", -withdrawal_amount)
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Emit a metric that is the authoritative value for a quantity at a point in time

        Examples:

        metrics.gauge(f"clickhouse.node_{hostname}.memory_usage", free_memory_in_bytes)
        """
        raise NotImplementedError

    @abstractmethod
    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Emit a metric for the timing performance of an operation.

        Example:

        metrics.timing("request.latency", request_latency_in_ms)
        """
        raise NotImplementedError

    @abstractmethod
    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Optional[Tags] = None,
    ) -> None:
        raise NotImplementedError
