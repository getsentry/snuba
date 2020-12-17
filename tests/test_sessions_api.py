from datetime import datetime, timedelta
from functools import partial

import pytz
import simplejson as json

from snuba import settings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from snuba.processor import MAX_UINT32
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


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

        self.storage = get_writable_storage(StorageKey.SESSIONS_RAW)
        self.generate_manual_session_events()

    def generate_manual_session_events(self):
        session_1 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aae"
        session_2 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf"
        user_1 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aae"
        user_2 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf"

        template = {
            "duration": MAX_UINT32,
            "environment": "production",
            "org_id": 1,
            "project_id": 1,
            "release": "sentry-test@1.0.0",
            "retention_days": settings.DEFAULT_RETENTION_DAYS,
            "seq": 0,
            "errors": 0,
            "received": datetime.now().isoformat(" ", "seconds"),
            "started": self.started.replace(tzinfo=None).isoformat(" ", "seconds"),
        }

        sessions = [
            # individual "exited" session with two updates, a user and errors
            {**template, "session_id": session_1, "distinct_id": user_1, "status": 0},
            {
                **template,
                "session_id": session_1,
                "distinct_id": user_1,
                "seq": 123,
                "status": 1,
                "errors": 123,
            },
            # individual "exited" session with just one update, no user, no errors
            {**template, "session_id": session_2, "status": 1},
            # pre-aggregated "errored" sessions, no user
            {**template, "quantity": 9, "status": 4},
            # pre-aggregated "exited" sessions with user
            {**template, "quantity": 5, "distinct_id": user_2, "status": 1},
            # pre-aggregated "exited" session
            {**template, "quantity": 4, "status": 1},
        ]

        self.storage.get_table_writer().get_batch_writer(
            metrics=DummyMetricsBackend(strict=True)
        ).write([json.dumps(session).encode("utf-8") for session in sessions])

    def test_manual_session_aggregation(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "sessions",
                    "organization": 1,
                    "project": 1,
                    "selected_columns": [
                        "sessions",
                        "sessions_errored",
                        "users",
                        "users_errored",
                    ],
                    "from_date": (self.started - self.skew).isoformat(),
                    "to_date": (self.started + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["sessions"] == 20
        assert data["data"][0]["sessions_errored"] == 10
        assert data["data"][0]["users"] == 2
        assert data["data"][0]["users_errored"] == 1
