from __future__ import annotations

from collections import ChainMap
from dataclasses import dataclass
from deprecation import deprecated
from typing import Mapping

from snuba.query.extensions import QueryExtensionData, TExtensionPayload
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings


@dataclass(frozen=True)
class Request:
    query: Query
    settings: RequestSettings  # settings provided by the request
    extensions: Mapping[str, QueryExtensionData[TExtensionPayload]]

    @property
    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods on the query object instead.")
    def body(self):
        raw_data = [extension.get_raw_data() for extension in self.extensions.values()]
        return ChainMap(self.query.get_body(), *raw_data)
