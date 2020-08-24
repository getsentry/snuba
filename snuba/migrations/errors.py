class InvalidMigrationState(Exception):
    pass


class MigrationDoesNotExist(Exception):
    pass


class MigrationError(Exception):
    pass


class MigrationInProgress(Exception):
    pass
