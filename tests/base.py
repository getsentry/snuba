import calendar
import os
import uuid
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from hashlib import md5
from typing import Iterator, MutableSequence, Optional, Sequence

from snuba import settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.dataset import Dataset
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.redis import redis_client
from snuba.writer import WriterTableRow
from tests.fixtures import raw_event


@contextmanager
def dataset_manager(name: str) -> Iterator[Dataset]:
    dataset = get_dataset(name)

    for storage in dataset.get_all_storages():
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )
        for statement in storage.get_schemas().get_drop_statements():
            clickhouse.execute(statement.statement)

        for statement in storage.get_schemas().get_create_statements():
            clickhouse.execute(statement.statement)

    try:
        yield dataset
    finally:
        for storage in dataset.get_all_storages():
            clickhouse = storage.get_cluster().get_query_connection(
                ClickhouseClientSettings.MIGRATE
            )
            for statement in storage.get_schemas().get_drop_statements():
                clickhouse.execute(statement.statement)


class BaseTest(object):
    def setup_method(self, test_method, dataset_name: Optional[str] = None):
        assert (
            settings.TESTING
        ), "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

        self.database = os.environ.get("CLICKHOUSE_DATABASE", "default")
        self.dataset_name = dataset_name

        if dataset_name is not None:
            self.__dataset_manager = dataset_manager(dataset_name)
            self.dataset = self.__dataset_manager.__enter__()
        else:
            self.__dataset_manager = None
            self.dataset = None

        redis_client.flushdb()

    def teardown_method(self, test_method):
        if self.__dataset_manager:
            self.__dataset_manager.__exit__(None, None, None)

        redis_client.flushdb()


class BaseDatasetTest(BaseTest):
    def write_processed_messages(self, messages: Sequence[ProcessedMessage]) -> None:
        rows: MutableSequence[WriterTableRow] = []
        for message in messages:
            assert isinstance(message, InsertBatch)
            rows.extend(message.rows)
        self.write_rows(rows)

    def write_rows(self, rows: Sequence[WriterTableRow]) -> None:
        enforce_table_writer(self.dataset).get_writer().write(rows)


class BaseEventsTest(BaseDatasetTest):
    def setup_method(self, test_method, dataset_name="events"):
        super(BaseEventsTest, self).setup_method(test_method, dataset_name)
        self.table = enforce_table_writer(self.dataset).get_schema().get_table_name()
        self.event = self.__get_event()

    def __get_event(self) -> InsertEvent:
        timestamp = datetime.utcnow()

        data = {
            "datetime": (timestamp - timedelta(seconds=2)).strftime(
                settings.PAYLOAD_DATETIME_FORMAT,
            ),
            "received": int(
                calendar.timegm((timestamp - timedelta(seconds=1)).timetuple())
            ),
            **deepcopy(raw_event),
        }

        unique = "%s:%s" % (str(data["project"]), data["id"])
        primary_hash = md5(unique.encode("utf-8")).hexdigest()

        return InsertEvent(
            {
                "event_id": data["id"],
                "group_id": int(primary_hash[:16], 16),
                "primary_hash": primary_hash,
                "project_id": data["project"],
                "message": data["message"],
                "platform": data["platform"],
                "datetime": data["datetime"],
                "data": data,
                "organization_id": data["organization_id"],
                "retention_days": settings.DEFAULT_RETENTION_DAYS,
            }
        )

    def create_event_row_for_date(
        self, dt: datetime, retention_days=settings.DEFAULT_RETENTION_DAYS
    ):
        return {
            "event_id": uuid.uuid4().hex,
            "project_id": 1,
            "group_id": 1,
            "deleted": 0,
            "timestamp": dt,
            "retention_days": retention_days,
        }

    def write_events(self, events: Sequence[InsertEvent]) -> None:
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )

        processed_messages = []
        for event in events:
            processed_message = processor.process_message((2, "insert", event, {}))
            assert processed_message is not None
            processed_messages.append(processed_message)

        self.write_processed_messages(processed_messages)


class BaseApiTest(BaseEventsTest):
    def setup_method(self, test_method, dataset_name="events"):
        super().setup_method(test_method, dataset_name)
        from snuba.web.views import application

        assert application.testing is True
        application.config["PROPAGATE_EXCEPTIONS"] = False
        self.app = application.test_client()
