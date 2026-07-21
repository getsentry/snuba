from enum import Enum


class AuditLogAction(Enum):
    # action.resource
    RAN_QUERY = "ran.query"
    RAN_MIGRATION_STARTED = "ran.migration.started"
    REVERSED_MIGRATION_STARTED = "reversed.migration.started"
    RAN_MIGRATION_COMPLETED = "ran.migration.completed"
    REVERSED_MIGRATION_COMPLETED = "reversed.migration.completed"
    RAN_MIGRATION_FAILED = "ran.migration.failed"
    REVERSED_MIGRATION_FAILED = "reversed.migration.failed"
    FORCE_MIGRATION_OVERWRITE = "force.migration.overwrite"
    RAN_SUDO_SYSTEM_QUERY = "ran.sudo.system.query"
    RAN_CLUSTERLESS_SYSTEM_QUERY = "ran.clusterless.system.query"


MIGRATION_ACTIONS = [
    AuditLogAction.RAN_MIGRATION_STARTED,
    AuditLogAction.REVERSED_MIGRATION_STARTED,
    AuditLogAction.RAN_MIGRATION_COMPLETED,
    AuditLogAction.REVERSED_MIGRATION_COMPLETED,
    AuditLogAction.RAN_MIGRATION_FAILED,
    AuditLogAction.REVERSED_MIGRATION_FAILED,
]
