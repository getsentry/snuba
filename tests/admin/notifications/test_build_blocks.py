from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.notifications.slack.utils import build_blocks


@pytest.mark.parametrize(
    "data,action,expected",
    [
        pytest.param(
            {
                "storage": "errors",
                "key": "org_limit_bytes_scanned_override",
                "value": "420",
                "params": "{'org_id': 1}",
            },
            AuditLogAction.ALLOCATION_POLICY_UPDATE,
            """*Storage errors Allocation Policy Changed:* :up: :date:\n\n```'org_limit_bytes_scanned_override({'org_id': 1})' = '420'```""",
            id="Allocation policy update",
        ),
        pytest.param(
            {
                "storage": "errors",
                "key": "org_limit_bytes_scanned_override",
                "params": "{'org_id': 1}",
            },
            AuditLogAction.ALLOCATION_POLICY_DELETE,
            """*Storage errors Allocation Policy Changed:* :put_litter_in_its_place:\n\n~```'org_limit_bytes_scanned_override({'org_id': 1})'```~""",
            id="Allocation policy delete",
        ),
    ],
)
def test_build_blocks(
    data: dict[str, Any], action: AuditLogAction, expected: str
) -> None:

    res = build_blocks(
        data,
        action,
        datetime.now().isoformat(),
        "ninja",
    )
    print(res[0]["text"]["text"])
    assert res[0]["text"]["text"] == expected
