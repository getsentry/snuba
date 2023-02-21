from snuba.datasets.factory import get_dataset
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


def test_limitby() -> None:
    body = {
        "query": """
            MATCH (transactions)
            SELECT tags.key, tags.value, event_id, group_id
            WHERE finish_ts >= toDateTime('2022-11-16T20:30:56.381367')
            AND finish_ts < toDateTime('2022-11-16T21:35:57.381367')
            AND project_id IN tuple(4550918988890113)
            AND project_id IN tuple(4550918988890113)
            AND group_id IN tuple(1)
            ORDER BY group_id ASC, timestamp ASC
            LIMIT 1 BY group_id
            """,
        "debug": True,
        "dry_run": True,
    }
    dataset = get_dataset("transactions")
    timer = Timer("something")
    referrer = "<unknown>"
    schema = RequestSchema.build(HTTPQuerySettings)

    request = build_request(
        body, parse_snql_query, HTTPQuerySettings, schema, dataset, timer, referrer
    )
    result = parse_and_run_query(dataset, request, timer)
    print(result)
    assert "LIMIT 1 BY _snuba_group_id" in result.extra["sql"]
