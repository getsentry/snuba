import logging
from collections.abc import Callable
from typing import NamedTuple

from snuba.migrations.status import Status


class Context(NamedTuple):
    migration_id: str
    logger: logging.Logger
    update_status: Callable[[Status], None]
