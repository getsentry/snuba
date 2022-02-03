from datetime import datetime

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.translator.query import identity_translate
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.snql.parser import parse_snql_query
from snuba.request.request_settings import HTTPRequestSettings


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
    query, _ = parse_snql_query(body, events)
    processors = events.get_default_entity().get_query_processors()
    for processor in processors:
        if isinstance(processor, TimeSeriesProcessor):
            processor.process_query(query, HTTPRequestSettings())

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
