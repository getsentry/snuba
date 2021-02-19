import itertools
import pytest
import pytz
from datetime import datetime, timedelta
import simplejson as json
from typing import Any, Callable, Tuple, Union


from snuba import settings, state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import MAX_UINT32
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


class TestSessionsApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "sessions"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    @pytest.fixture(scope="class")
    def get_project_id(self, request: object) -> Callable[[], int]:
        id_iter = itertools.count()
        next(id_iter)  # skip 0
        return lambda: next(id_iter)

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        # values for test data
        self.minutes = 180
        self.skew = timedelta(minutes=self.minutes)
        self.started = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        )

        self.storage = get_writable_storage(StorageKey.SESSIONS_RAW)

    def generate_manual_session_events(self, project_id: int) -> None:
        session_1 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aae"
        session_2 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf"
        user_1 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aae"
        user_2 = "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf"

        template = {
            "duration": MAX_UINT32,
            "environment": "production",
            "org_id": 1,
            "project_id": project_id,
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

    def test_manual_session_aggregation(
        self, get_project_id: Callable[[], int]
    ) -> None:
        project_id = get_project_id()
        self.generate_manual_session_events(project_id)
        response = self.post(
            json.dumps(
                {
                    "dataset": "sessions",
                    "organization": 1,
                    "project": project_id,
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

    def generate_session_events(self, project_id: int) -> None:
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        template = {
            "session_id": "00000000-0000-0000-0000-000000000000",
            "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
            "duration": None,
            "environment": "production",
            "org_id": 1,
            "project_id": project_id,
            "release": "sentry-test@1.0.0",
            "retention_days": settings.DEFAULT_RETENTION_DAYS,
            "seq": 0,
            "errors": 0,
            "received": datetime.utcnow().timestamp(),
            "started": self.started.timestamp(),
        }
        events = [
            processor.process_message(
                {
                    **template,
                    "status": "exited",
                    "duration": 1947.49,
                    "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
                    "started": (self.started + timedelta(minutes=13)).timestamp(),
                },
                meta,
            ),
            processor.process_message(
                {**template, "status": "exited", "quantity": 5}, meta,
            ),
            processor.process_message(
                {**template, "status": "errored", "errors": 1, "quantity": 2}, meta,
            ),
            processor.process_message(
                {
                    **template,
                    "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                    "status": "errored",
                    "errors": 1,
                    "quantity": 2,
                    "started": (self.started + timedelta(minutes=24)).timestamp(),
                },
                meta,
            ),
        ]
        filtered = [e for e in events if e]
        write_processed_messages(self.storage, filtered)

    def test_session_aggregation(self, get_project_id: Callable[[], int]) -> None:
        project_id = get_project_id()
        self.generate_session_events(project_id)
        response = self.post(
            json.dumps(
                {
                    "dataset": "sessions",
                    "organization": 1,
                    "project": project_id,
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
        assert data["data"][0]["sessions"] == 10
        assert data["data"][0]["sessions_errored"] == 4
        assert data["data"][0]["users"] == 1
        assert data["data"][0]["users_errored"] == 1

    # the test sessions have timestamps of `:00`, `:13` and `:24`, so they will
    # end up in 3 buckets no matter if we group by minute, 2 minutes or 10 minutes
    @pytest.mark.parametrize("granularity", [60, 120, 600])
    def test_session_small_granularity(
        self, get_project_id: Callable[[], int], granularity: int
    ):
        state.set_config("allow_subhour_sessions", 1)
        project_id = get_project_id()
        self.generate_session_events(project_id)
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "sessions",
                    "organization": 1,
                    "project": project_id,
                    "selected_columns": [
                        "bucketed_started",
                        "sessions",
                        "sessions_errored",
                        "users",
                        "users_errored",
                    ],
                    "groupby": ["bucketed_started"],
                    "orderby": ["bucketed_started"],
                    "granularity": granularity,
                    "from_date": (self.started - self.skew).isoformat(),
                    "to_date": (self.started + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data

        assert len(data["data"]) == 3, data
        assert data["data"][0]["sessions"] == 7
        assert data["data"][0]["sessions_errored"] == 2
        assert data["data"][0]["users"] == 1
        assert data["data"][0]["users_errored"] == 1
        assert data["data"][1]["sessions"] == 1
        assert data["data"][1]["sessions_errored"] == 0
        assert data["data"][1]["users"] == 1
        assert data["data"][1]["users_errored"] == 0
        assert data["data"][2]["sessions"] == 2
        assert data["data"][2]["sessions_errored"] == 2
        assert data["data"][2]["users"] == 1
        assert data["data"][2]["users_errored"] == 1
