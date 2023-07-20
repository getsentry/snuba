from snuba.utils.serializable_exception import SerializableException


class InvalidMigrationState(SerializableException):
    pass


class MigrationDoesNotExist(SerializableException):
    pass


class MigrationError(SerializableException):
    pass


class MigrationInProgress(SerializableException):
    pass


class InvalidClickhouseVersion(SerializableException):
    pass


class InactiveClickhouseReplica(SerializableException):
    pass
