from snuba.clickhouse.query_dsl.accessors import get_project_ids_in_query_ast
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings
from snuba.state import get_configs
from snuba.state.rate_limit import PROJECT_RATE_LIMIT_NAME, RateLimitParameters


class ProjectRateLimiterProcessor(QueryProcessor):
    """
    If there isn't already a rate limiter on a project, search the top level
    conditions for project IDs using the given project column name and add a
    rate limiter for them.
    """

    def __init__(self, project_column: str) -> None:
        self.project_column = project_column

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # If the settings don't already have a project rate limit, add one
        existing = request_settings.get_rate_limit_params()
        for ex in existing:
            if ex.rate_limit_name == PROJECT_RATE_LIMIT_NAME:
                return

        project_ids = get_project_ids_in_query_ast(query, self.project_column)
        if not project_ids:
            return

        project_id = project_ids.pop()

        prl, pcl = get_configs(
            [("project_per_second_limit", 1000), ("project_concurrent_limit", 1000)]
        )

        # Specific projects can have their rate limits overridden
        (per_second, concurr) = get_configs(
            [
                ("project_per_second_limit_{}".format(project_id), prl),
                ("project_concurrent_limit_{}".format(project_id), pcl),
            ]
        )

        rate_limit = RateLimitParameters(
            rate_limit_name=PROJECT_RATE_LIMIT_NAME,
            bucket=str(project_id),
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )

        request_settings.add_rate_limit(rate_limit)
