from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor


class SpansV2MessageProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("SpansV2MessageProcessor")
