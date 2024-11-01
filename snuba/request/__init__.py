from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, Union

from snuba.attribution.attribution_info import AttributionInfo
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import LogicalDataSource, Table
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


@dataclass(frozen=True)
class Request:
    id: uuid.UUID
    original_body: Dict[str, Any]
    query: Union[Query, CompositeQuery[LogicalDataSource]]
    query_settings: QuerySettings
    attribution_info: AttributionInfo

    @property
    def referrer(self) -> str:
        return self.attribution_info.referrer


@dataclass(frozen=True)
class DeleteRequest:
    id: str
    query: ProcessableQuery[Table]
    storage: str
    where_clause: str
