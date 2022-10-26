from __future__ import annotations

from datetime import datetime
from typing import TypedDict

import structlog

NotificationData = TypedDict("NotificationData", {"query": str})


class QuerylogAuditLogClient:
    def __init__(self) -> None:
        self.logger = structlog.get_logger().bind(module=__name__)

    def notify(
        self,
        data: NotificationData,
        user: str,
        timestamp: str | None = None,
    ) -> None:
        self.logger.info(
            event="run_query",
            user=user,
            query=data["query"],
            timestamp=timestamp or datetime.now().strftime("%B %d, %Y %H:%M:%S %p"),
        )
