from datetime import datetime

import pytest

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.translator.query import identity_translate
from snuba.query.logical import Query
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query


@pytest.mark.redis_db
def test_get_time_range() -> None:
    """
    Test finding the time range of a query.
    """
    body = """
        MATCH (events)
        SELECT event_id
        WHERE timestamp >= toDateTime('2019-09-18T10:00:00')
            AND timestamp >= toDateTime('2000-09-18T10:00:00')
            AND timestamp < toDateTime('2019-09-19T12:00:00')
            AND (timestamp < toDateTime('2019-09-18T12:00:00') OR project_id IN tuple(1))
            AND project_id IN tuple(1)
        """

    events = get_dataset("events")
    entity = get_entity(EntityKey.EVENTS)
    query, _ = parse_snql_query(body, events)
    assert isinstance(query, Query)
    processors = entity.get_query_processors()
    for processor in processors:
        if isinstance(processor, TimeSeriesProcessor):
            processor.process_query(query, HTTPQuerySettings())

    from_date_ast, to_date_ast = get_time_range(identity_translate(query), "timestamp")
    assert (
        from_date_ast is not None
        and isinstance(from_date_ast, datetime)
        and from_date_ast.isoformat() == "2019-09-18T10:00:00"
    )
    assert (
        to_date_ast is not None
        and isinstance(to_date_ast, datetime)
        and to_date_ast.isoformat() == "2019-09-19T12:00:00"
    )
