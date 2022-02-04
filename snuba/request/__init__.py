from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Union

from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.request.request_settings import RequestSettings


@dataclass(frozen=True)
class Request:
    id: str
    body: Mapping[str, Any]
    query: Union[Query, CompositeQuery[Entity]]
    snql_anonymized: str
    settings: RequestSettings  # settings provided by the request

    @property
    def referrer(self) -> str:
        return self.settings.referrer
