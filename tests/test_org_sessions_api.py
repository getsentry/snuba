import itertools
from datetime import datetime, timedelta
from typing import Any, Callable

import pytest
import pytz
import simplejson as json
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.expressions import Granularity
from snuba_sdk.query import Column, Entity, Function, Query

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


class TestOrgSessionsApi(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

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

    def generate_session_events(self, project_id: int) -> None:
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        template = {
            "session_id": "00000000-0000-0000-0000-000000000001",
            # "distinct_id": "bmeow211-58a4-4b36-a9a1-5a55df0d9aaf",
            "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
            "duration": None,
            "environment": "production",
            "org_id": 1,
            "project_id": project_id,
            "release": "sentry-test@1.0.1",
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

    def test_simple(self, get_project_id: Callable[[], int]) -> None:
        project_id = get_project_id()
        self.generate_session_events(project_id)
        project_id2 = get_project_id()
        self.generate_session_events(project_id2)

        query = Query(
            dataset="org_sessions",
            match=Entity("org_sessions"),
            select=[
                Column("org_id"),
                Function("groupUniqArray", [Column("project_id")], "project_ids"),
            ],
            groupby=[Column("org_id")],
            where=[
                Condition(
                    Column("started"), Op.GTE, datetime.utcnow() - timedelta(hours=6)
                ),
                Condition(Column("started"), Op.LT, datetime.utcnow()),
            ],
            granularity=Granularity(3600),
            # offset=Offset(0),
            # limit=Limit(10),
        )

        response = self.app.post("/org_sessions/snql", data=query.snuba(),)
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1
        assert data["data"][0]["org_id"] == 1
        assert data["data"][0]["project_ids"] == [project_id2, project_id]
