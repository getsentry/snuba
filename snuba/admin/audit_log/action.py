from enum import Enum


class AuditLogAction(Enum):
    # action.resource
    ADDED_OPTION = "added.option"
    REMOVED_OPTION = "removed.option"
    UPDATED_OPTION = "updated.option"
    RAN_QUERY = "ran.query"
    RAN_MIGRATION = "ran.migration"
    REVERSED_MIGRATION = "reversed.migration"


RUNTIME_CONFIG_ACTIONS = [
    AuditLogAction.ADDED_OPTION,
    AuditLogAction.REMOVED_OPTION,
    AuditLogAction.UPDATED_OPTION,
]
