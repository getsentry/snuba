import pytest

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job
from snuba.manual_jobs.scrub_users_from_eap_spans_str_attrs import (
    ScrubUserFromEAPSpansStrAttrs,
)


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_basic() -> None:
    job_id = "abc"
    run_job(
        JobSpec(
            job_id,
            "ScrubUserFromEAPSpansStrAttrs",
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
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, "b"],
                "start_datetime": "2024-12-01 00:00:00",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubUserFromEAPSpansStrAttrs",
            False,
            {
                "organization_ids": [1, 2],
                "start_datetime": "2024-12-01 00:00:0",
                "end_datetime": "2024-12-10 00:00:00",
            },
        ),
        JobSpec(
            "abc",
            "ScrubUserFromEAPSpansStrAttrs",
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
    job = ScrubUserFromEAPSpansStrAttrs(
        JobSpec(
            "bassa",
            "ScrubUserFromEAPSpansStrAttrs",
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
        == """ALTER TABLE spans_str_attrs_3_local

DELETE WHERE (attr_key = 'sentry.user.ip' OR attr_key = 'sentry.user')
AND organization_id IN [1,3,5,6]
AND timestamp >= toDateTime('2024-12-01T00:00:00')
AND timestamp < toDateTime('2024-12-10T00:00:00')"""
    )
