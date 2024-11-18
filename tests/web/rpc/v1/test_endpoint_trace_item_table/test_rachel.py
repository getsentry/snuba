import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, TypedDict, Union
from uuid import uuid4

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.timestamp_pb2 import Timestamp as ProtobufTimestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from tests.base import BaseApiTest


def before_now(**kwargs: float) -> datetime:
    date = datetime.now(UTC) - timedelta(**kwargs)
    return date - timedelta(microseconds=date.microsecond % 1000)


BASE_TIME = datetime.now(timezone.utc).replace(
    minute=0, second=0, microsecond=0
) - timedelta(minutes=180)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestRachel(BaseApiTest):
    def create_span(
        self,
        extra_data: dict[str, Any] | None = None,
        start_ts: datetime | None = None,
        duration: int = 1000,
        measurements: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create span json, not required for store_span, but with no params passed should just work out of the box"""
        if start_ts is None:
            start_ts = datetime.now() - timedelta(days=30)
        if extra_data is None:
            extra_data = {}
        span: dict = {
            "is_segment": False,
            "measurements": {},
            "retention_days": 90,
            "sentry_tags": {},
            "tags": {},
        }
        # Load some defaults
        span.update(
            {
                "event_id": uuid4().hex,
                "organization_id": 4555051977080832,
                "project_id": 4555051977211905,
                "trace_id": uuid4().hex,
                "span_id": uuid4().hex[:16],
                "parent_span_id": uuid4().hex[:16],
                "segment_id": uuid4().hex[:16],
                "group_raw": uuid4().hex[:16],
                "profile_id": uuid4().hex,
                # Multiply by 1000 cause it needs to be ms
                "start_timestamp_ms": int(start_ts.timestamp() * 1000),
                "start_timestamp_precise": start_ts.timestamp(),
                "end_timestamp_precise": start_ts.timestamp() + duration / 1000,
                "timestamp": int(start_ts.timestamp() * 1000),
                "received": start_ts.timestamp(),
                "duration_ms": duration,
                "exclusive_time_ms": duration,
            }
        )
        # Load any specific custom data
        span.update(extra_data)
        # coerce to string
        for tag, value in dict(span["tags"]).items():
            span["tags"][tag] = str(value)
        if measurements:
            span["measurements"] = measurements
        return span

    def store_spans(self, spans, is_eap=False):
        for span in spans:
            span["ingest_in_eap"] = is_eap
        assert (
            self.app.post(
                f"/tests/entities/{'eap_' if is_eap else ''}spans/insert",
                data=json.dumps(spans),
            ).status_code
            == 200
        )

    def test_rachel(self):
        self.store_spans(
            [
                self.create_span(
                    {
                        "description": "foo",
                        "sentry_tags": {"status": "success"},
                        "tags": {"foo": "five"},
                    },
                    measurements={"foo": {"value": 5}},
                    start_ts=before_now(minutes=10),
                ),
            ],
            is_eap=True,
        )

        # response = self.do_request(
        #     {
        #         "field": ["description", "tags[foo,number]", "tags[foo,string]", "tags[foo]"],
        #         "query": "",
        #         "orderby": "description",
        #         "project": 4555051977080832,
        #         "dataset": "spans",
        #     }
        # )

        req = TraceItemTableRequest(
            meta=RequestMeta(
                organization_id=4555051977080832,
                referrer="api.organization-events",
                project_ids=[4555051977211905],
                start_timestamp=Timestamp(
                    seconds=int(
                        datetime(
                            year=2024,
                            month=8,
                            day=17,
                            hour=19,
                            minute=19,
                            second=13,
                            microsecond=417691,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
                end_timestamp=Timestamp(
                    seconds=int(
                        datetime(
                            year=2024,
                            month=11,
                            day=15,
                            hour=19,
                            minute=29,
                            second=13,
                            microsecond=417691,
                            tzinfo=UTC,
                        ).timestamp()
                    )
                ),
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.name"),
                    label="description",
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_INT, name="foo"),
                    label="tags[foo,number]",
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="foo"),
                    label="tags[foo,string]",
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="foo"),
                    label="tags[foo]",
                ),
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.span_id"
                    ),
                    label="id",
                ),
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="project.name"
                    ),
                    label="project.name",
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.name"
                        ),
                        label="description",
                    )
                )
            ],
            group_by=[
                AttributeKey(type="TYPE_STRING", name="sentry.name"),
                AttributeKey(type="TYPE_INT", name="foo"),
                AttributeKey(type="TYPE_STRING", name="foo"),
                AttributeKey(type="TYPE_STRING", name="foo"),
                AttributeKey(type="TYPE_STRING", name="sentry.span_id"),
                AttributeKey(type="TYPE_STRING", name="project.name"),
            ],
            virtual_column_contexts=[
                VirtualColumnContext(
                    from_column_name="sentry.project_id",
                    to_column_name="project.name",
                    value_map={"4555051977211905": "bar"},
                )
            ],
        )

        response = self.app.post(
            "/rpc/EndpointTraceItemTable/v1",
            data=req.SerializeToString(),
            headers={"referer": "api.organization-events"},
        )
        #
        # print("jdflksflkslf")
        # print(response.data)
        # #print(json.loads(response.data))
        # print(response.status_code)
        # print(response.json)

        assert False
        assert response.status_code == 200, response.content
        datata = data["data"]
        assert datata[0]["data"]["tags[foo,number]"] == 5
        assert datata[0]["tags[foo,string]"] == "five"
        assert datata[0]["tags[foo]"] == "five"
