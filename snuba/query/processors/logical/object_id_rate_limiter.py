from typing import Optional

from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_config
from snuba.state.rate_limit import (
    ORGANIZATION_RATE_LIMIT_NAME,
    PROJECT_RATE_LIMIT_NAME,
    PROJECT_REFERRER_RATE_LIMIT_NAME,
    REFERRER_RATE_LIMIT_NAME,
    RateLimitParameters,
    get_rate_limit_config,
)

DEFAULT_LIMIT = 1000


class ObjectIDRateLimiterProcessor(LogicalQueryProcessor):
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
        query_settings_field: Optional[str] = None,
        default_limit: int = DEFAULT_LIMIT,
    ) -> None:
        self.object_column = object_column
        self.rate_limit_name = rate_limit_name
        self.per_second_name = per_second_name
        self.concurrent_name = concurrent_name
        self.query_settings_field = query_settings_field
        self.default_limit = default_limit

    def _is_already_applied(self, query_settings: QuerySettings) -> bool:
        existing = query_settings.get_rate_limit_params()
        for ex in existing:
            if ex.rate_limit_name == self.rate_limit_name:
                return True
        return False

    def get_object_id(
        self, query: Query, query_settings: QuerySettings
    ) -> Optional[str]:
        obj_ids = get_object_ids_in_query_ast(query, self.object_column)
        if not obj_ids:
            return None

        # TODO: Add logic for multiple IDs
        obj_id = str(obj_ids.pop())
        if self.query_settings_field is not None:
            query_settings_field_val = getattr(
                query_settings, self.query_settings_field, None
            )
            if query_settings_field_val is not None:
                obj_id = f"{obj_id}_{query_settings_field_val}"
        return str(obj_id)

    def get_per_second_name(self, query: Query, query_settings: QuerySettings) -> str:
        return self.per_second_name

    def get_concurrent_name(self, query: Query, query_settings: QuerySettings) -> str:
        return self.concurrent_name

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        if not get_config("enable_legacy_ratelimiters", 1):
            return
        # If the settings don't already have an object rate limit, add one
        if self._is_already_applied(query_settings):
            return
        per_second_name = self.get_per_second_name(query, query_settings)
        concurrent_name = self.get_concurrent_name(query, query_settings)
        object_rate_limit, object_concurrent_limit = get_rate_limit_config(
            (per_second_name, self.default_limit),
            (concurrent_name, self.default_limit),
        )
        obj_id = self.get_object_id(query, query_settings)
        if obj_id is None:
            return
        # Specific objects can have their rate limits overridden
        (per_second, concurr) = get_rate_limit_config(
            (f"{per_second_name}_{obj_id}", object_rate_limit),
            (f"{concurrent_name}_{obj_id}", object_concurrent_limit),
        )

        rate_limit = RateLimitParameters(
            rate_limit_name=self.rate_limit_name,
            bucket=str(obj_id),
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )

        query_settings.add_rate_limit(rate_limit)


class OnlyIfConfiguredRateLimitProcessor(ObjectIDRateLimiterProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # If the settings don't already have an object rate limit, add one
        per_second_name = self.get_per_second_name(query, query_settings)
        concurrent_name = self.get_concurrent_name(query, query_settings)

        if self._is_already_applied(query_settings):
            return
        object_rate_limit, object_concurrent_limit = get_rate_limit_config(
            (per_second_name, None), (concurrent_name, None)
        )
        obj_id = self.get_object_id(query, query_settings)
        if obj_id is None:
            return
        # don't enforce any limit that isn't specified
        (per_second, concurr) = get_rate_limit_config(
            (f"{per_second_name}_{obj_id}", object_rate_limit),
            (f"{concurrent_name}_{obj_id}", object_concurrent_limit),
        )

        # Specific objects can have their rate limits overridden
        if per_second is None and concurr is None:
            return
        rate_limit = RateLimitParameters(
            rate_limit_name=self.rate_limit_name,
            bucket=str(obj_id),
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )

        query_settings.add_rate_limit(rate_limit)


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


class ProjectReferrerRateLimiter(OnlyIfConfiguredRateLimitProcessor):
    def __init__(self, project_column: str) -> None:
        super().__init__(
            project_column,
            PROJECT_REFERRER_RATE_LIMIT_NAME,
            "project_referrer_per_second_limit",
            "project_referrer_concurrent_limit",
            query_settings_field="referrer",
        )

    def get_concurrent_name(self, query: Query, query_settings: QuerySettings) -> str:
        return f"{self.concurrent_name}_{query_settings.referrer}"

    def get_per_second_name(self, query: Query, query_settings: QuerySettings) -> str:
        return f"{self.per_second_name}_{query_settings.referrer}"

    def get_object_id(
        self, query: Query, query_settings: QuerySettings
    ) -> Optional[str]:
        obj_ids = get_object_ids_in_query_ast(query, self.object_column)
        if not obj_ids:
            return None

        # TODO: Add logic for multiple IDs
        obj_id = str(obj_ids.pop())
        return str(obj_id)


class ReferrerRateLimiterProcessor(OnlyIfConfiguredRateLimitProcessor):
    """This is more of a load shedder than a rate limiter. we limit a specific
    referrer regardless of customer"""

    def __init__(self) -> None:
        super().__init__(
            "nocolumn",
            REFERRER_RATE_LIMIT_NAME,
            "referrer_per_second_limit",
            "referrer_concurrent_limit",
            query_settings_field="referrer",
        )

    def get_object_id(self, query: Query, query_settings: QuerySettings) -> str:
        return query_settings.referrer


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
