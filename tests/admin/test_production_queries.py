from snuba.admin.production_queries import prod_queries
from snuba.datasets.factory import get_dataset


def test_validate_projects():
    query = """MATCH {MATCH (events) SELECT time, group_id, count() AS event_count BY time, group_id WHERE timestamp >= toDateTime('2023-11-20T16:02:34.565803') AND timestamp < toDateTime('2023-11-27T16:02:34.565803') AND project_id=1 HAVING event_count > 1 ORDER BY time ASC GRANULARITY 3600} SELECT quantiles(90)(event_count) BY group_id WHERE timestamp >= toDateTime('2023-11-20T16:02:34.565803') AND timestamp < toDateTime('2023-11-27T16:02:34.565803') AND project_id=1"""
    prod_queries._validate_projects_in_query(
        body={"query": query, "dataset": "events"}, dataset=get_dataset("events")
    )
