import calendar
import uuid

from datetime import datetime

from snuba import settings
from snuba.consumer import KafkaMessageMetadata
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from tests.base import BaseEventsTest


class TestTransactionsDataset(BaseEventsTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)

        self.project_id = self.event["project_id"]
        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.span_id = "8841662216cc598b"
        self.url = "https://poutine.co/needscurds"
        self.generate_transaction()

    def generate_transaction(self):
        self.dataset = get_dataset("transactions")

        processed = (
            enforce_table_writer(self.dataset)
            .get_stream_loader()
            .get_processor()
            .process_message(
                (
                    2,
                    "insert",
                    {
                        "project_id": self.project_id,
                        "event_id": uuid.uuid4().hex,
                        "deleted": 0,
                        "datetime": (self.base_time).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "platform": "python",
                        "retention_days": settings.DEFAULT_RETENTION_DAYS,
                        "data": {
                            "received": calendar.timegm((self.base_time).timetuple()),
                            "type": "transaction",
                            "transaction": "/api/do_things",
                            "start_timestamp": datetime.timestamp(self.base_time),
                            "timestamp": datetime.timestamp(self.base_time),
                            "tags": {
                                # Sentry
                                "environment": "prød",
                                "sentry:release": "1",
                                "sentry:dist": "dist1",
                                # User
                                "foo": "baz",
                                "foo.bar": "qux",
                                "os_name": "linux",
                                "url": self.url,
                            },
                            "user": {
                                "email": "sally@example.org",
                                "ip_address": "8.8.8.8",
                                "geo": {
                                    "city": "San Francisco",
                                    "region": "CA",
                                    "country_code": "US",
                                },
                            },
                            "contexts": {
                                "trace": {
                                    "trace_id": self.trace_id.hex,
                                    "span_id": self.span_id,
                                    "op": "http",
                                },
                                "device": {
                                    "online": True,
                                    "charging": True,
                                    "model_id": "Galaxy",
                                },
                            },
                            "sdk": {
                                "name": "sentry.python",
                                "version": "0.13.4",
                                "integrations": ["django"],
                            },
                            "spans": [
                                {
                                    "op": "db",
                                    "trace_id": self.trace_id.hex,
                                    "span_id": self.span_id + "1",
                                    "parent_span_id": None,
                                    "same_process_as_parent": True,
                                    "description": "SELECT * FROM users",
                                    "data": {},
                                    "timestamp": calendar.timegm(
                                        (self.base_time).timetuple()
                                    ),
                                }
                            ],
                            "request": {
                                "url": self.url,
                                "method": "GET",
                                "referer": "https://canadianoutrage.com",
                            },
                        },
                    },
                ),
                KafkaMessageMetadata(0, 0, self.base_time),
            )
        )

        self.write_processed_messages([processed])

    def test_url(self) -> None:
        """
        Adds an event and ensures the url is properly populated
        including escaping.
        """
        # self.generate_transaction()

        clickhouse = (
            get_storage(StorageKey.TRANSACTIONS)
            .get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
        )

        event = clickhouse.execute(
            f"SELECT url FROM transactions_local WHERE url = '{self.url}'"
        )
        print(event)
        assert len(event) == 1
        assert event[0][0] == self.url
