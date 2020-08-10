class MigrationError(Exception):
    pass


class MigrationInProgress(Exception):
    pass


class InvalidMigrationState(Exception):
    pass
