import calendar
import os
import uuid
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from hashlib import md5
from typing import Any, Iterator, Mapping, Optional, Sequence, Union

from snuba import settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.datasets.events_processor_base import InsertEvent
from snuba.processor import ProcessorAction, ProcessedMessage
from snuba.redis import redis_client


def is_raw_event(data: Union[Mapping[str, Any], InsertEvent]) -> bool:
    return not data.keys() == InsertEvent.__annotations__.keys()


def wrap_raw_event(data: Mapping[str, Any]) -> InsertEvent:
    "Wrap a raw event like the Sentry codebase does before sending to Kafka."

    unique = "%s:%s" % (str(data["project"]), data["id"])
    primary_hash = md5(unique.encode("utf-8")).hexdigest()

    return {
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


def get_event() -> InsertEvent:
    from tests.fixtures import raw_event

    timestamp = datetime.utcnow()
    raw_event["datetime"] = (timestamp - timedelta(seconds=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    raw_event["received"] = int(
        calendar.timegm((timestamp - timedelta(seconds=1)).timetuple())
    )
    return wrap_raw_event(deepcopy(raw_event))


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
    def write_processed_records(self, records):
        if not isinstance(records, (list, tuple)):
            records = [records]

        rows = []
        for event in records:
            rows.append(event)

        return self.write_rows(rows)

    def write_rows(self, rows):
        if not isinstance(rows, (list, tuple)):
            rows = [rows]
        enforce_table_writer(self.dataset).get_writer().write(rows)


class BaseEventsTest(BaseDatasetTest):
    def setup_method(self, test_method, dataset_name="events"):
        super(BaseEventsTest, self).setup_method(test_method, dataset_name)
        self.table = enforce_table_writer(self.dataset).get_schema().get_table_name()
        self.event = get_event()

    def create_event_for_date(self, dt, retention_days=settings.DEFAULT_RETENTION_DAYS):
        event = {
            "event_id": uuid.uuid4().hex,
            "project_id": 1,
            "group_id": 1,
            "deleted": 0,
        }
        event["timestamp"] = dt
        event["retention_days"] = retention_days
        return event

    def write_raw_events(self, events):
        out = []
        for event in events:
            if is_raw_event(event):
                event = wrap_raw_event(event)

            processed = (
                enforce_table_writer(self.dataset)
                .get_stream_loader()
                .get_processor()
                .process_message(event)
            )
            assert processed is not None
            out.append(processed)

        self.write_processed_events(out)

    def write_processed_events(self, events: Sequence[ProcessedMessage]) -> None:
        rows = []
        for event in events:
            assert event.action is ProcessorAction.INSERT
            rows.extend(event.data)
        self.write_rows(rows)

    def write_rows(self, rows) -> None:
        if not isinstance(rows, (list, tuple)):
            rows = [rows]

        enforce_table_writer(self.dataset).get_writer().write(rows)


class BaseApiTest(BaseEventsTest):
    def setup_method(self, test_method, dataset_name="events"):
        super().setup_method(test_method, dataset_name)
        from snuba.web.views import application

        assert application.testing is True
        application.config["PROPAGATE_EXCEPTIONS"] = False
        self.app = application.test_client()
