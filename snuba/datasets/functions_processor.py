from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import MessageProcessor, ProcessedMessage


class FunctionsMessageProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        # TODO
        return None
