from __future__ import annotations

import logging
from datetime import timezone
from typing import Any, Optional

import simplejson as json

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage, ReplacementBatch

logger = logging.getLogger(__name__)


class RustCompatProcessor(DatasetMessageProcessor):
    def __init__(self, processor_name: str):
        import rust_snuba

        self.__process_message = rust_snuba.process_message  # type: ignore
        self.__processor_name = processor_name

    def process_message(
        self, message: Any, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        insert_payload, replacement_payload = self.__process_message(
            self.__processor_name,
            json.dumps(message).encode("utf8"),
            metadata.partition,
            metadata.offset,
            int(metadata.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000),
        )

        if insert_payload is not None:
            assert replacement_payload is None

            rows = [
                json.loads(line)
                for line in insert_payload.rstrip(b"\n").split(b"\n")
                if line
            ]

            return InsertBatch(
                rows=rows,
                origin_timestamp=None,
                sentry_received_timestamp=None,
            )
        elif replacement_payload is not None:
            assert insert_payload is None
            key, values_bytes = replacement_payload

            values = [
                json.loads(line)
                for line in values_bytes.rstrip(b"\n").split(b"\n")
                if line
            ]
            return ReplacementBatch(key=key.decode("utf8"), values=values)
        else:
            raise ValueError("unsupported return value from snuba_rust")
