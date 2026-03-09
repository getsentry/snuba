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
    FORCE_MIGRATION_OVERWRITE = "force.migration.overwrite"
    CONFIGURABLE_COMPONENT_UPDATE = "configurable_component.update"
    CONFIGURABLE_COMPONENT_DELETE = "configurable_component.delete"
    DLQ_REPLAY = "dlq.replay"
    RAN_SUDO_SYSTEM_QUERY = "ran.sudo.system.query"
    RAN_CLUSTERLESS_SYSTEM_QUERY = "ran.clusterless.system.query"


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

CONFIGURABLE_COMPONENT_ACTIONS = [
    AuditLogAction.CONFIGURABLE_COMPONENT_DELETE,
    AuditLogAction.CONFIGURABLE_COMPONENT_UPDATE,
]
