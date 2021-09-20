from snuba.utils.snuba_exception import SnubaException


class InvalidMigrationState(SnubaException):
    pass


class MigrationDoesNotExist(SnubaException):
    pass


class MigrationError(SnubaException):
    pass


class MigrationInProgress(SnubaException):
    pass


class InvalidClickhouseVersion(SnubaException):
    pass
