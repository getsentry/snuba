from typing import Optional, Sequence

from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import get_dataset
from snuba.processor import ProcessedMessage
from tests.helpers import write_processed_messages, write_unprocessed_events


class BaseDatasetTest:
    def setup_method(self, test_method, dataset_name: Optional[str] = None):
        self.dataset_name = dataset_name

        if dataset_name is not None:
            self.dataset = get_dataset(dataset_name)
        else:
            self.dataset = None

    def write_processed_messages(self, messages: Sequence[ProcessedMessage]) -> None:
        storage = self.dataset.get_writable_storage()
        assert storage is not None
        write_processed_messages(storage, messages)

    def write_unprocessed_events(self, events: Sequence[InsertEvent]) -> None:
        storage = self.dataset.get_writable_storage()
        assert storage is not None

        write_unprocessed_events(storage, events)


class BaseApiTest(BaseDatasetTest):
    def setup_method(self, test_method, dataset_name="events"):
        super().setup_method(test_method, dataset_name)
        from snuba.web.views import application

        assert application.testing is True
        application.config["PROPAGATE_EXCEPTIONS"] = False
        self.app = application.test_client()
