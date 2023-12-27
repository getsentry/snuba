from __future__ import annotations

import logging

from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor

logger = logging.getLogger(__name__)


class QuerylogProcessor(RustCompatProcessor):
    def __init__(self):
        super().__init__("QuerylogProcessor")
