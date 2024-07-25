from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableMapping, Optional, Sequence

from snuba.query.query_settings import QuerySettings
from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters

MAX_ROWS_TO_DELETE_DEFAULT = 1000


@dataclass
class DeletionSettings:
    is_enabled: int
    tables: Sequence[str]
    max_rows_to_delete: int = MAX_ROWS_TO_DELETE_DEFAULT


class DeletionQuerySettings(QuerySettings):
    def __init__(self, deletion_settings: DeletionSettings):
        super().__init__()
        self.deletion_settings = deletion_settings

    def get_turbo(self) -> bool:
        return False

    def get_consistent(self) -> bool:
        return False

    def get_debug(self) -> bool:
        return False

    def get_dry_run(self) -> bool:
        return False

    def get_legacy(self) -> bool:
        return False

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return []

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        pass

    def get_resource_quota(self) -> Optional[ResourceQuota]:
        return None

    def set_resource_quota(self, quota: ResourceQuota) -> None:
        pass

    def get_clickhouse_settings(self) -> MutableMapping[str, Any]:
        return {}

    def set_clickhouse_settings(self, settings: MutableMapping[str, Any]) -> None:
        pass

    def get_asynchronous(self) -> bool:
        return False
