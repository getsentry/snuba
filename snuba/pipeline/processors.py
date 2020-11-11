import sentry_sdk

from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.request import Request


def _execute_query_plan_processors(
    query_plan: ClickhouseQueryPlan, request: Request
) -> None:
    for clickhouse_processor in query_plan.plan_processors:
        with sentry_sdk.start_span(
            description=type(clickhouse_processor).__name__, op="processor"
        ):
            clickhouse_processor.process_query(query_plan.query, request.settings)


def _execute_entity_processors(request: Request) -> None:
    entity = get_entity(request.query.get_from_clause().key)

    for processor in entity.get_query_processors():
        with sentry_sdk.start_span(
            description=type(processor).__name__, op="processor"
        ):
            processor.process_query(request.query, request.settings)
