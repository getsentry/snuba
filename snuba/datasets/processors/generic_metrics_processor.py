from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor


class GenericSetsMetricsProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("GenericSetsMetricsProcessor")


class GenericDistributionsMetricsProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("GenericDistributionsMetricsProcessor")


class GenericCountersMetricsProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("GenericCountersMetricsProcessor")


class GenericGaugesMetricsProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("GenericGaugesMetricsProcessor")
