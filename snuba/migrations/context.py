import logging
from typing import Callable, NamedTuple

from snuba.migrations.status import Status


class MigrationContext(NamedTuple):
    migration_id: str
    logger: logging.Logger
    update_status: Callable[[Status], None]


class OperationContext(NamedTuple):
    single_node: bool
    table_name: str
