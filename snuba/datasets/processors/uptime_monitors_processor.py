import logging

from snuba import environment
from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "uptime_monitor_checks.processor")


class UptimeMonitorChecksProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("UptimeMonitorChecksProcessor")
