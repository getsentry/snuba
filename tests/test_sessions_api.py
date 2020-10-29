from datetime import datetime, timedelta
from functools import partial

import pytz
import simplejson as json

from snuba import settings, state
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


class TestSessionsApi(BaseApiTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        self.app.post = partial(self.app.post, headers={"referer": "test"})

        # values for test data
        self.minutes = 180
        self.skew = timedelta(minutes=self.minutes)
        self.started = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        )

        self.storage = get_writable_storage(StorageKey.SESSIONS)
        self.generate_session_events()

    def teardown_method(self, test_method):
        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def generate_session_events(self):
        """
        Generate a deterministic set of events.
        """
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        events = [
            processor.process_message(
                {
                    "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                    "duration": 1947.49,
                    "environment": "production",
                    "org_id": 1,
                    "project_id": 1,
                    "release": "sentry-test@1.0.0",
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                    "seq": 0,
                    "errors": 0,
                    "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
                    "started": self.started.timestamp(),
                    "status": "exited",
                },
                meta,
            ),
            processor.process_message(
                {
                    "aggregates": [
                        {"started": self.started.timestamp(), "exited": 5},
                        {"started": self.started.timestamp(), "errored": 2},
                        {
                            "started": self.started.timestamp(),
                            "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                            "errored": 2,
                        },
                    ],
                    "environment": "production",
                    "org_id": 1,
                    "project_id": 1,
                    "release": "sentry-test@1.0.0",
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                },
                meta,
            ),
        ]
        write_processed_messages(self.storage, events)

    def test_session_aggregation(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "sessions",
                    "project": 1,
                    "selected_columns": [
                        "sessions",
                        "sessions_errored",
                        "users",
                        "users_errored",
                    ],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 1, data
        assert data["data"][0]["sessions"] == 10
        assert data["data"][0]["sessions_errored"] == 4
        assert data["data"][0]["users"] == 1
        assert data["data"][0]["users_errored"] == 1
