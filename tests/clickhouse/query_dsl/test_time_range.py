from datetime import datetime

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.translator.query import identity_translate
from snuba.query.parser import parse_query
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.request.request_settings import HTTPRequestSettings


def test_get_time_range() -> None:
    """
    Test finding the time range of a query.
    """
    body = {
        "selected_columns": ["event_id"],
        "conditions": [
            ("timestamp", ">=", "2019-09-18T10:00:00"),
            ("timestamp", ">=", "2000-09-18T10:00:00"),
            ("timestamp", "<", "2019-09-19T12:00:00"),
            [("timestamp", "<", "2019-09-18T12:00:00"), ("project_id", "IN", [1])],
            ("project_id", "IN", [1]),
        ],
    }

    events = get_dataset("events")
    query = parse_query(body, events)
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
