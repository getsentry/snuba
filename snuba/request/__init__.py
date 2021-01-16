from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping, Union

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
    body: Mapping[str, Any]
    query: Union[Query, CompositeQuery[Entity]]
    settings: RequestSettings  # settings provided by the request
    referrer: str
    preprocessor: RequestPreprocessor


# Allows the parser and validator to provide some operations
# to be performed on the query at the beginning of the pipeline
# that are not dataset specific.
RequestPreprocessor = Callable[[Query, RequestSettings], None]
