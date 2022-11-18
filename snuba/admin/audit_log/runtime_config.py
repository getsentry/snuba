from typing import Any, Mapping

from snuba.admin.audit_log.base import AuditLog


class RuntimeConfigAuditLog(AuditLog):
    def _format_data(
        self, user: str, timestamp: str, action: str, data: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """
        Data has the runtime option, its old value, and new value.
        If it's being removed, the new value will be `None`. Likewise
        if it's being added, the old value will be `None`.

        example:
            {
                "option": "enable_events_read_only_table",
                "old": 0, // or None if added
                "new": 1, // or None if removed
            }

        """
        return {
            "option": data.get("option"),
            "old": data.get("old"),
            "new": data.get("new"),
        }


runtime_config_auditlog = RuntimeConfigAuditLog(__name__)
