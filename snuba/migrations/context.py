import logging
from typing import Callable, NamedTuple

from snuba.migrations.status import Status


class Context(NamedTuple):
    migration_id: str
    logger: logging.Logger
    update_status: Callable[[Status], None]
