import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import run_job
from tests.helpers import write_raw_unprocessed_events


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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
@pytest.mark.skip(reason="can't test writing to GCS")
def test_extract_span_data() -> None:
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

    assert (
        run_job(
            JobSpec(
                "jobid",
                "ExtractSpanData",
                False,
                {
                    "organization_ids": [0, 1],
                    "start_timestamp": (BASE_TIME - timedelta(minutes=30)).isoformat(),
                    "end_timestamp": (BASE_TIME + timedelta(hours=24)).isoformat(),
                    "table_name": "snuba_test.eap_spans_2_local",
                    "limit": 1000000,
                    "output_file_path": "scrubbed_spans_data.csv.gz",
                    "gcp_bucket_name": "test-bucket",
                    "allowed_keys": ["sentry.span.op"],
                },
            )
        )
        == JobStatus.FINISHED
    )
