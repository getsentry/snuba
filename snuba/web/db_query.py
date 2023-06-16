from __future__ import annotations

from typing import Any, MutableMapping, MutableSequence, Optional, Union

from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.clickhouse.query import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import QuerySettings
from snuba.querylog.query_metadata import ClickhouseQueryMetadata
from snuba.reader import Reader
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.db_query_class import DBQuery


def db_query(
    clickhouse_query: Union[Query, CompositeQuery[Table]],
    query_settings: QuerySettings,
    attribution_info: AttributionInfo,
    dataset_name: str,
    # NOTE: This variable is a piece of state which is updated and used outside this function
    query_metadata_list: MutableSequence[ClickhouseQueryMetadata],
    formatted_query: FormattedQuery,
    reader: Reader,
    timer: Timer,
    # NOTE: This variable is a piece of state which is updated and used outside this function
    stats: MutableMapping[str, Any],
    trace_id: Optional[str] = None,
    robust: bool = False,
) -> QueryResult:
    return DBQuery(
        clickhouse_query=clickhouse_query,
        query_settings=query_settings,
        attribution_info=attribution_info,
        dataset_name=dataset_name,
        formatted_query=formatted_query,
        reader=reader,
        timer=timer,
        query_metadata_list=query_metadata_list,
        stats=stats,
        trace_id=trace_id,
        robust=robust,
    ).db_query()
