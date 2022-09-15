import logging
from typing import Callable, NamedTuple, Optional

from snuba.migrations.status import Status


class Context(NamedTuple):
    migration_id: str
    logger: logging.Logger
    update_status: Callable[[Status], None]
    partition_id: Optional[int]
