from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Union

from snuba.attribution.attribution_info import AttributionInfo
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


@dataclass(frozen=True)
class Request:
    id: str
    original_body: Dict[str, Any]
    query: Union[Query, CompositeQuery[Entity]]
    query_settings: QuerySettings
    attribution_info: AttributionInfo

    # TODO: This should maybe not live on the request
    snql_anonymized: str

    @property
    def referrer(self) -> str:
        return self.attribution_info.referrer
