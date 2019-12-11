from __future__ import annotations

from collections import ChainMap
from dataclasses import dataclass
from deprecation import deprecated
from typing import Any, Mapping

from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings


@dataclass(frozen=True)
class Request:
    query: Query
    settings: RequestSettings  # settings provided by the request
    extensions: Mapping[str, Mapping[str, Any]]
    referrer: str

    @property
    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods on the query object instead."
    )
    def body(self):
        return ChainMap(self.query.get_body(), *self.extensions.values())
