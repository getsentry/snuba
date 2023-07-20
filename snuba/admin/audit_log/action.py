from enum import Enum


class AuditLogAction(Enum):
    # action.resource
    ADDED_OPTION = "added.option"
    REMOVED_OPTION = "removed.option"
    UPDATED_OPTION = "updated.option"
    RAN_QUERY = "ran.query"
    RAN_MIGRATION_STARTED = "ran.migration.started"
    REVERSED_MIGRATION_STARTED = "reversed.migration.started"
    RAN_MIGRATION_COMPLETED = "ran.migration.completed"
    REVERSED_MIGRATION_COMPLETED = "reversed.migration.completed"
    RAN_MIGRATION_FAILED = "ran.migration.failed"
    REVERSED_MIGRATION_FAILED = "reversed.migration.failed"
    ALLOCATION_POLICY_UPDATE = "allocation_policy.update"
    ALLOCATION_POLICY_DELETE = "allocation_policy.delete"
    DLQ_REPLAY = "dlq.replay"


RUNTIME_CONFIG_ACTIONS = [
    AuditLogAction.ADDED_OPTION,
    AuditLogAction.REMOVED_OPTION,
    AuditLogAction.UPDATED_OPTION,
]

MIGRATION_ACTIONS = [
    AuditLogAction.RAN_MIGRATION_STARTED,
    AuditLogAction.REVERSED_MIGRATION_STARTED,
    AuditLogAction.RAN_MIGRATION_COMPLETED,
    AuditLogAction.REVERSED_MIGRATION_COMPLETED,
    AuditLogAction.RAN_MIGRATION_FAILED,
    AuditLogAction.REVERSED_MIGRATION_FAILED,
]

ALLOCATION_POLICY_ACTIONS = [
    AuditLogAction.ALLOCATION_POLICY_DELETE,
    AuditLogAction.ALLOCATION_POLICY_UPDATE,
]
