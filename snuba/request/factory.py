import uuid
from typing import Any

from snuba.attribution.attribution_info import AttributionInfo
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.request import Request


def create_request(
    original_body: dict[str, Any],
    query: Query | CompositeQuery[Entity],
    query_settings: QuerySettings,
    attribution_info: AttributionInfo,
    snql_anonymized: str,
) -> Request:
    return Request(
        id=uuid.uuid4().hex,
        original_body=original_body,
        query=query,
        attribution_info=attribution_info,
        query_settings=query_settings,
        snql_anonymized=snql_anonymized,
    )
