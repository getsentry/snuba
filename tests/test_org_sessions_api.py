import itertools
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
import simplejson as json
from snuba_sdk import Request
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Granularity
from snuba_sdk.orderby import Direction, OrderBy
from snuba_sdk.query import Query

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestOrgSessionsApi(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None) -> None:
        # values for test data
        self.started = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )

        self.storage = get_writable_storage(StorageKey.SESSIONS_RAW)

        self.id_iter = itertools.count()
        next(self.id_iter)  # skip 0
        self.project_id = next(self.id_iter)
        self.org_id = next(self.id_iter)
        self.project_id2 = next(self.id_iter)

        self.generate_session_events(self.org_id, self.project_id)
        self.generate_session_events(self.org_id, self.project_id2)

    def generate_session_events(self, org_id: int, project_id: int) -> None:
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        distinct_id = uuid4().hex
        template = {
            "session_id": uuid4().hex,
            "distinct_id": distinct_id,
            "duration": None,
            "environment": "production",
            "org_id": org_id,
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
                    "session_id": uuid4().hex,
                    "started": (self.started + timedelta(minutes=13)).timestamp(),
                },
                meta,
            ),
            processor.process_message(
                {**template, "status": "exited", "quantity": 5},
                meta,
            ),
            processor.process_message(
                {**template, "status": "errored", "errors": 1, "quantity": 2},
                meta,
            ),
            processor.process_message(
                {
                    **template,
                    "distinct_id": distinct_id,
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

    def test_simple(self) -> None:
        query = Query(
            match=Entity("org_sessions"),
            select=[Column("org_id"), Column("project_id")],
            groupby=[Column("org_id"), Column("project_id")],
            where=[
                Condition(
                    Column("started"), Op.GTE, datetime.utcnow() - timedelta(hours=6)
                ),
                Condition(Column("started"), Op.LT, datetime.utcnow()),
            ],
            granularity=Granularity(3600),
        )

        request = Request(dataset="sessions", app_id="default", query=query)
        response = self.app.post(
            "/sessions/snql",
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 2
        assert data["data"][0]["org_id"] == self.org_id
        assert data["data"][0]["project_id"] == self.project_id
        assert data["data"][1]["org_id"] == self.org_id
        assert data["data"][1]["project_id"] == self.project_id2

    def test_orderby(self) -> None:
        self.project_id3 = next(self.id_iter)
        self.org_id2 = next(self.id_iter)
        self.generate_session_events(self.org_id2, self.project_id3)

        query = Query(
            match=Entity("org_sessions"),
            select=[Column("org_id"), Column("project_id")],
            groupby=[Column("org_id"), Column("project_id")],
            where=[
                Condition(
                    Column("started"), Op.GTE, datetime.utcnow() - timedelta(hours=6)
                ),
                Condition(Column("started"), Op.LT, datetime.utcnow()),
            ],
            granularity=Granularity(3600),
            orderby=[OrderBy(Column("org_id"), Direction.ASC)],
        )

        request = Request(dataset="sessions", app_id="default", query=query)
        response = self.app.post(
            "/sessions/snql",
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 3
        assert data["data"][0]["org_id"] == self.org_id
        assert data["data"][0]["project_id"] == self.project_id
        assert data["data"][1]["org_id"] == self.org_id
        assert data["data"][1]["project_id"] == self.project_id2
        assert data["data"][2]["org_id"] == self.org_id2
        assert data["data"][2]["project_id"] == self.project_id3

        query = query.set_orderby(
            [OrderBy(Column("org_id"), Direction.DESC)],
        )

        request = Request(dataset="sessions", app_id="default", query=query)
        response = self.app.post(
            "/sessions/snql",
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 3
        assert data["data"][0]["org_id"] == self.org_id2
        assert data["data"][0]["project_id"] == self.project_id3
        assert data["data"][1]["org_id"] == self.org_id
        assert data["data"][1]["project_id"] == self.project_id
        assert data["data"][2]["org_id"] == self.org_id
        assert data["data"][2]["project_id"] == self.project_id2
