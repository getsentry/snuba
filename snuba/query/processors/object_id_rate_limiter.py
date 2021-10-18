from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings
from snuba.state import get_configs
from snuba.state.rate_limit import (
    ORGANIZATION_RATE_LIMIT_NAME,
    PROJECT_RATE_LIMIT_NAME,
    RateLimitParameters,
)


class ObjectIDRateLimiterProcessor(QueryProcessor):
    """
    A generic rate limiter that searches a query for conditions on the given column.
    The values in that column are assumed to be an integer ID.
    """

    def __init__(
        self,
        object_column: str,
        rate_limit_name: str,
        per_second_name: str,
        concurrent_name: str,
    ) -> None:
        self.object_column = object_column
        self.rate_limit_name = rate_limit_name
        self.per_second_name = per_second_name
        self.concurrent_name = concurrent_name

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # If the settings don't already have an object rate limit, add one
        existing = request_settings.get_rate_limit_params()
        for ex in existing:
            if ex.rate_limit_name == self.rate_limit_name:
                return

        obj_ids = get_object_ids_in_query_ast(query, self.object_column)
        if not obj_ids:
            return

        # TODO: Add logic for multiple IDs
        obj_id = obj_ids.pop()

        orl, ocl = get_configs(
            [(self.per_second_name, 1000), (self.concurrent_name, 1000)]
        )

        # Specific objects can have their rate limits overridden
        (per_second, concurr) = get_configs(
            [
                (f"{self.per_second_name}_{obj_id}", orl),
                (f"{self.concurrent_name}_{obj_id}", ocl),
            ]
        )

        rate_limit = RateLimitParameters(
            rate_limit_name=self.rate_limit_name,
            bucket=str(obj_id),
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )

        request_settings.add_rate_limit(rate_limit)


class OrganizationRateLimiterProcessor(ObjectIDRateLimiterProcessor):
    """
    If there isn't already a rate limiter on an organization, search the top level
    conditions for an organization ID using the given organization column name and add a
    rate limiter for them.
    """

    def __init__(self, org_column: str) -> None:
        super().__init__(
            org_column,
            ORGANIZATION_RATE_LIMIT_NAME,
            "org_per_second_limit",
            "org_concurrent_limit",
        )


class ProjectRateLimiterProcessor(ObjectIDRateLimiterProcessor):
    """
    If there isn't already a rate limiter on a project, search the top level
    conditions for project IDs using the given project column name and add a
    rate limiter for them.
    """

    def __init__(self, project_column: str) -> None:
        super().__init__(
            project_column,
            PROJECT_RATE_LIMIT_NAME,
            "project_per_second_limit",
            "project_concurrent_limit",
        )
