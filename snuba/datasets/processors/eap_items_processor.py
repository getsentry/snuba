from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor


class EAPItemsProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("EAPItemsProcessor")
