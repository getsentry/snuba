from unittest.mock import Mock, PropertyMock, patch

from structlog.testing import capture_logs

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog


@patch("snuba.admin.audit_log.base.SlackClient")
def test_audit_log(mock_slack_client: Mock) -> None:
    audit_log = AuditLog()
    client = mock_slack_client()
    is_configured = PropertyMock(return_value=True)
    type(client).is_configured = is_configured

    data = {"option": "word", "old": "blah", "new": "blep"}
    with capture_logs() as cap_logs:
        audit_log.record("meredith@sentry.io", AuditLogAction.UPDATED_OPTION, data=data)

    assert len(cap_logs) == 1
    log = cap_logs[0]
    assert log["user"] == "meredith@sentry.io"
    assert log["event"] == "updated.option"
    assert "timestamp" in log

    assert client.post_message.call_count == 0
    audit_log.record("meredith@sentry.io", AuditLogAction.UPDATED_OPTION, data=data, notify=True)
    assert client.post_message.call_count == 1
