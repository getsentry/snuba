import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ExistsFilter, TraceItemFilter
from sentry_protos.snuba.v1.trace_item_table_column_pb2 import Column

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job
from snuba.manual_jobs.scrub_ips_from_eap_spans import ScrubIpFromEAPSpans
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_USER_IP = "192.168.0.45"


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_basic() -> None:
    job_id = "abc"
    run_job(
        JobSpec(
            job_id,
            "ScrubIpFromEAPSpans",
            False,
            {
                "organization_ids": [1, 3, 5, 6],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        )
    )

    assert get_job_status(job_id) == JobStatus.FINISHED


@pytest.mark.parametrize(
    ("jobspec"),
    [
        JobSpec(
            "abc",
            "ScrubIpFromEAPSpans",
            False,
            {
                "organization_ids": [1, "b"],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubIpFromEAPSpans",
            False,
            {
                "organization_ids": [1, 2],
                "start_datetime": "2024-12-01 00:00:0",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubIpFromEAPSpans",
            False,
            {
                "organization_ids": [1, 2],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:0",
            },
        ),
    ],
)
@pytest.mark.redis_db
def test_fail_validation(jobspec: JobSpec) -> None:
    with pytest.raises(Exception):
        run_job(jobspec)


@pytest.mark.redis_db
def test_generate_query() -> None:
    job = ScrubIpFromEAPSpans(
        JobSpec(
            "bassa",
            "ScrubIpFromEAPSpans",
            False,
            {
                "organization_ids": [1, 3, 5, 6],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        )
    )
    assert (
        job._get_query(None)
        == """ALTER TABLE eap_spans_2_local

UPDATE `attr_str_14` = mapApply((k, v) -> (k, if(k = 'sentry.user.ip', 'scrubbed', v)), `attr_str_14`)
WHERE organization_id IN [1,3,5,6]
AND _sort_timestamp >= toDateTime('2024-12-01T00:00:00')
AND _sort_timestamp < toDateTime('2024-12-10T00:00:00')"""
    )


def _gen_message(
    dt: datetime,
    organization_id: int,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
) -> Mapping[str, Any]:
    measurements = measurements or {}
    tags = tags or {}
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": _RELEASE_TAG,
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "sentry.segment.name": "/api/0/relays/projectconfigs/",
            "sentry.sdk.name": "sentry.python.django",
            "sentry.sdk.version": "2.7.0",
            "my.float.field": 101.2,
            "my.int.field": 2000,
            "my.neg.field": -100,
            "my.neg.float.field": -101.2,
            "my.true.bool.field": True,
            "my.false.bool.field": False,
        },
        "measurements": {
            "num_of_spans": {"value": 50.0},
            "eap.measurement": {"value": random.choice([1, 100, 1000])},
            **measurements,
        },
        "organization_id": organization_id,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": "http.server",
            "platform": "python",
            "release": _RELEASE_TAG,
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
            "status": "ok",
            "status_code": "200",
            "thread.id": "8522009600",
            "thread.name": "uWSGIWorker1Core0",
            "trace.status": "ok",
            "transaction": "/api/0/relays/projectconfigs/",
            "transaction.method": "POST",
            "transaction.op": "http.server",
            "user": "ip:127.0.0.1",
            "user.ip": _USER_IP,
        },
        "span_id": "123456781234567D",
        "tags": {
            "http.status_code": "200",
            "relay_endpoint_version": "3",
            "relay_id": "88888888-4444-4444-8444-cccccccccccc",
            "relay_no_cache": "False",
            "relay_protocol_version": "3",
            "relay_use_post_or_schedule": "True",
            "relay_use_post_or_schedule_rejected": "version",
            "spans_over_limit": "False",
            "server_name": "blah",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
            **tags,
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


def _generate_request(
    ts: Any, hour_ago: int, organization_id: int, project_ids: list[int]
) -> TraceItemTableRequest:
    # project_ids is added as an argument to avoid this query getting cached
    return TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=project_ids,
            organization_id=organization_id,
            cogs_category="something",
            referrer="something",
            start_timestamp=Timestamp(seconds=hour_ago),
            end_timestamp=ts,
            request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        filter=TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
            )
        ),
        columns=[
            Column(
                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.user.ip")
            )
        ],
        order_by=[
            TraceItemTableRequest.OrderBy(
                column=Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.user.ip"
                    )
                )
            )
        ],
    )


def _generate_expected_response(ip: str) -> TraceItemTableResponse:
    return TraceItemTableResponse(
        column_values=[
            TraceItemColumnValues(
                attribute_name="sentry.user.ip",
                results=[AttributeValue(val_str=ip) for _ in range(20)],
            )
        ],
        page_token=PageToken(offset=20),
        meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_span_is_scrubbed() -> None:
    BASE_TIME = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(minutes=180)
    organization_ids = [0, 1]
    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [
        _gen_message(BASE_TIME - timedelta(minutes=i), organization_id)
        for organization_id in organization_ids
        for i in range(20)
    ]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore

    # we inserted spans for organizations 0, 1, 2, and we make sure they look as expected
    ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
    hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
    for organization_id in organization_ids:

        response = EndpointTraceItemTable().execute(
            _generate_request(ts, hour_ago, organization_id, [1, 2, 3])
        )
        assert response == _generate_expected_response(_USER_IP)

    # next we scrub organizations 0
    start_datetime = datetime.utcfromtimestamp(Timestamp(seconds=hour_ago).seconds)
    end_datetime = datetime.utcfromtimestamp(ts.seconds)

    run_job(
        JobSpec(
            "plswork",
            "ScrubIpFromEAPSpans",
            False,
            {
                "organization_ids": [organization_ids[0]],
                "start_datetime": start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "end_datetime": end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
    )

    response = EndpointTraceItemTable().execute(
        _generate_request(ts, hour_ago, organization_ids[0], [3, 2, 1])
    )
    assert response == _generate_expected_response("scrubbed")

    # then we make sure organization 1 is NOT SCRUBBED
    response = EndpointTraceItemTable().execute(
        _generate_request(ts, hour_ago, organization_ids[1], [3, 2, 1])
    )
    assert response == _generate_expected_response(_USER_IP)
