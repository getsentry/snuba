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
                "resource_identifier": "errors",
                "configurable_component_class_name": "BytesScannedWindowAllocationPolicy",
                "key": "org_limit_bytes_scanned_override",
                "value": "420",
                "params": "{'org_id': 1}",
            },
            AuditLogAction.CONFIGURABLE_COMPONENT_UPDATE,
            """*Resource errors Configurable Component BytesScannedWindowAllocationPolicy Changed:* :up: :date:\n\n```'BytesScannedWindowAllocationPolicy.org_limit_bytes_scanned_override({'org_id': 1})' = '420'```""",
            id="Allocation policy update",
        ),
        pytest.param(
            {
                "resource_identifier": "errors",
                "configurable_component_class_name": "BytesScannedWindowAllocationPolicy",
                "key": "org_limit_bytes_scanned_override",
                "params": "{'org_id': 1}",
            },
            AuditLogAction.CONFIGURABLE_COMPONENT_DELETE,
            """*Resource errors Configurable Component BytesScannedWindowAllocationPolicy Changed:* :put_litter_in_its_place:\n\n~```'BytesScannedWindowAllocationPolicy.org_limit_bytes_scanned_override({'org_id': 1})'```~""",
            id="Allocation policy delete",
        ),
    ],
)
def test_build_blocks(data: dict[str, Any], action: AuditLogAction, expected: str) -> None:

    res = build_blocks(
        data,
        action,
        datetime.now().isoformat(),
        "ninja",
    )
    assert res[0]["text"]["text"] == expected
