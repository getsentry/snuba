from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import simplejson as json

from snuba import settings
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestEAPSpansAPI(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    def basic_span_data(self, start_time: datetime) -> dict[str, Any]:
        # missing span_id, parent_span_id, trace_id
        return {
            "duration_ms": 10,
            "exclusive_time_ms": 9,
            "is_segment": True,
            "project_id": 70156,
            "organization_id": 1,
            "received": datetime.now().timestamp(),
            "retention_days": 90,
            "segment_id": "1234567890123456",
            "sentry_tags": {
                "transaction.method": "GET",
                "user": "user1",
                "release": "release1234",
            },
            "start_timestamp": start_time.strftime(settings.PAYLOAD_DATETIME_FORMAT),
            "start_timestamp_ms": int(start_time.timestamp() * 1000),
            "start_timestamp_precise": start_time.timestamp(),
            "end_timestamp": (start_time + timedelta(milliseconds=10)).strftime(
                settings.PAYLOAD_DATETIME_FORMAT
            ),
            "end_timestamp_precise": (
                start_time + timedelta(milliseconds=10)
            ).timestamp(),
        }

    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None, redis_db: None) -> None:
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83adaa")
        self.raw_span_id = "7400045b25c443b%d"

        spans = []
        self.start_time = datetime.now(tz=UTC) - timedelta(minutes=10)
        for i in range(10):
            span_id = self.raw_span_id % i
            parent_span_id = self.raw_span_id % (i - 1) if i > 0 else None
            start_time = self.start_time + timedelta(minutes=i)
            span_data = self.basic_span_data(start_time)
            span_data["trace_id"] = str(self.trace_id)
            span_data["span_id"] = span_id
            span_data["parent_span_id"] = parent_span_id
            spans.append(span_data)

        self.project_id = spans[0]["project_id"]
        self.organization_id = spans[0]["organization_id"]
        spans_storage = get_entity(EntityKey.EAP_SPANS).get_writable_storage()
        assert spans_storage is not None
        write_raw_unprocessed_events(spans_storage, spans)

    def test_simple_query(self) -> None:
        response = self.post(
            "/events_analytics_platform/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (eap_spans)
                    SELECT span_id
                    WHERE organization_id = {self.organization_id}
                    AND start_timestamp >= toDateTime('{(self.start_time - timedelta(minutes=20)).isoformat()}')
                    AND start_timestamp < toDateTime('{(self.start_time + timedelta(minutes=20)).isoformat()}')
                    AND trace_id = '{self.trace_id}'
                    """,
                    "referrer": "myreferrer",
                    "debug": True,
                    "tenant_ids": {
                        "referrer": "r",
                        "organization_id": self.organization_id,
                    },
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert len(data["data"]) == 10
        span_ids = set([span["span_id"] for span in data["data"]])
        for i in range(10):
            assert self.raw_span_id % i in span_ids

    def test_attribute_meta_query(self) -> None:
        response = self.post(
            "/events_analytics_platform/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH STORAGE(spans_attributes_meta)
                    SELECT attribute_value BY attribute_value
                    WHERE organization_id = {self.organization_id}
                    AND attribute_key = 'release'
                    """,
                    "referrer": "myreferrer",
                    "debug": True,
                    "tenant_ids": {
                        "referrer": "r",
                        "organization_id": self.organization_id,
                    },
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert len(data["data"]) == 1
        assert data["data"][0]["attribute_value"] == "release1234"
