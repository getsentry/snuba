from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.notifications.slack.utils import build_blocks


def test_build_blocks_labels_adhoc_manual_job() -> None:
    blocks = build_blocks(
        {
            "job_id": "ToyJob_abc",
            "job_type": "ToyJob",
            "status": "finished",
            "adhoc": True,
            "params": "{}",
        },
        AuditLogAction.RAN_MANUAL_JOB,
        "2026-01-01T00:00:00.000000Z",
        "operator@sentry.io",
    )

    text = blocks[0]["text"]["text"]
    assert "ad hoc" in text
    assert "one-shot" not in text
    assert "params" not in text


def test_build_blocks_labels_oneshot_manual_job() -> None:
    blocks = build_blocks(
        {
            "job_id": "abc1234",
            "job_type": "ToyJob",
            "status": "failed",
            "adhoc": False,
            "params": '{"p1": "value1"}',
        },
        AuditLogAction.RAN_MANUAL_JOB,
        "2026-01-01T00:00:00.000000Z",
        "operator@sentry.io",
    )

    text = blocks[0]["text"]["text"]
    assert "one-shot" in text
    assert "ad hoc" not in text
    assert "[FAILED]" in text
    assert "params" not in text
    assert "value1" not in text


def test_build_blocks_omits_customer_identifiers_from_slack() -> None:
    blocks = build_blocks(
        {
            "job_id": "scrub-1",
            "job_type": "ScrubUserFromEAPSpans",
            "status": "finished",
            "adhoc": False,
            "params": '{"organization_ids": [42], "project_ids": [99]}',
        },
        AuditLogAction.RAN_MANUAL_JOB,
        "2026-01-01T00:00:00.000000Z",
        "operator@sentry.io",
    )

    text = blocks[0]["text"]["text"]
    assert "organization_ids" not in text
    assert "project_ids" not in text
    assert "42" not in text
    assert "99" not in text
    assert "ScrubUserFromEAPSpans" in text
    assert "scrub-1" in text
