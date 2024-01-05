from __future__ import annotations

import logging
from typing import Any, Optional

import simplejson as json

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage

logger = logging.getLogger(__name__)


class RustCompatProcessor(DatasetMessageProcessor):
    def __init__(self, processor_name: str):
        import rust_snuba

        self.__process_message = rust_snuba.process_message  # type: ignore
        self.__processor_name = processor_name

    def process_message(
        self, message: Any, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        rust_processed_message = bytes(
            self.__process_message(
                self.__processor_name,
                json.dumps(message).encode("utf8"),
                metadata.partition,
                metadata.offset,
                int(metadata.timestamp.timestamp() * 1000),
            )
        )

        rows = [
            json.loads(line)
            for line in rust_processed_message.rstrip(b"\n").split(b"\n")
            if line
        ]

        return InsertBatch(
            rows=rows,
            origin_timestamp=None,
            sentry_received_timestamp=None,
        )
