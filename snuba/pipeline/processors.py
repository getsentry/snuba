from typing import Tuple

import sentry_sdk
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.pipeline import Segment
from snuba.query.logical import Query as LogicalQuery
from snuba.request.request_settings import RequestSettings
from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder


ClickhouseProcessingPayload = Tuple[ClickhouseQueryPlan, RequestSettings]


class QueryPlanProcessorsExecutor(Segment[ClickhouseProcessingPayload, None]):
    """
    Executes the Clickhouse query processor for the query. These
    are taken from the plan.

    This segment does not return anything because we still work with
    the assumption that the query is transformed in place.
    """

    def execute(self, processing_payload: ClickhouseProcessingPayload) -> None:
        query_plan, settings = processing_payload
        for clickhouse_processor in query_plan.plan_processors:
            with sentry_sdk.start_span(
                description=type(clickhouse_processor).__name__, op="processor"
            ):
                clickhouse_processor.process_query(query_plan.query, settings)


EntityProcessingPayload = Tuple[LogicalQuery, RequestSettings]


class EntityProcessingExecutor(Segment[EntityProcessingPayload, None]):
    """
    Executes the entity query processors for the query. These are taken
    from the entity.

    This segment does not return anything because we still work with
    the assumption that the query is transformed in place.
    """

    def execute(self, processing_payload: EntityProcessingPayload) -> None:
        query, settings = processing_payload
        entity = get_entity(query.get_from_clause().key)

        for processor in entity.get_query_processors():
            with sentry_sdk.start_span(
                description=type(processor).__name__, op="processor"
            ):
                processor.process_query(query, settings)


class QueryProcessingPipeline(Segment[EntityProcessingPayload, ClickhouseQueryPlan]):
    def __init__(self, query_plan_builder: ClickhouseQueryPlanBuilder) -> None:
        self.__query_plan_builder = query_plan_builder
        self.__entity_processors = EntityProcessingExecutor()
        self.__clickhouse_processors = QueryPlanProcessorsExecutor()

    def execute(self, query_payload: EntityProcessingPayload) -> ClickhouseQueryPlan:
        self.__entity_processors.execute(query_payload)

        query, settings = query_payload
        query_plan = self.__query_plan_builder.build_plan(query, settings)
        self.__clickhouse_processors.execute((query_plan, settings))
        return query_plan
