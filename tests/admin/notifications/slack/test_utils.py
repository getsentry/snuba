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

    assert "ad hoc" in blocks[0]["text"]["text"]
    assert "one-shot" not in blocks[0]["text"]["text"]


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
