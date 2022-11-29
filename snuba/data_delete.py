from enum import Enum

import structlog

from snuba.clickhouse.native import ClickhousePool

logger = structlog.get_logger().bind(module=__name__)


class DeleteResult(Enum):
    IN_PROGRESS = 0
    FAILED = 1
    SUCCEEDED = 2


def data_delete(
    connection: ClickhousePool,
    delete_query_without_partition: str,
    dry_run: bool = True,
) -> DeleteResult:
    pass
