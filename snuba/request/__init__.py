from __future__ import annotations

from collections import ChainMap
from dataclasses import dataclass
from deprecation import deprecated
from enum import Enum
from typing import Any, Mapping, Union

from snuba.query.logical import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.request.request_settings import RequestSettings


class Language(Enum):
    """
    Which language is being used in the Snuba request.
    """

    LEGACY = "legacy"
    SNQL = "snql"


@dataclass(frozen=True)
class Request:
    id: str
    query: Union[Query, CompositeQuery[Entity]]
    settings: RequestSettings  # settings provided by the request
    extensions: Mapping[str, Mapping[str, Any]]
    referrer: str
    language: Language

    @property
    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods on the query object instead."
    )
    def body(self):
        assert isinstance(self.query, Query)
        return ChainMap(self.query.get_body(), *self.extensions.values())
