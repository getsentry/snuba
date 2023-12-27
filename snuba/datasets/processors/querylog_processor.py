from __future__ import annotations

from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor


class QuerylogProcessor(RustCompatProcessor):
    def __init__(self):
        super().__init__("QuerylogProcessor")
