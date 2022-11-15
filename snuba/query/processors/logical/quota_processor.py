from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_config
from snuba.state.quota import ResourceQuota

ENABLED_CONFIG = "resource_quota_processor_enabled"
REFERRER_PROJECT_CONFIG = "referrer_project_thread_quota"
REFERRER_CONFIG = "referrer_thread_quota"


class ResourceQuotaProcessor(LogicalQueryProcessor):
    """
    Applies a referrer/project thread quota to the query. Can throttle a referrer
    or a (referrer, project) pair. The more specific restriction takes precedence

    Example:

        SET referrer_thread_quota_MYREFERRER = 20
        # all requests with referrer = MYREFERRER are now capped at 20 threads

        SET referrer_project_thread_quota_MYREFERRER_1337 = 2
        # all requests with referrer = MYREFERRER, project_id = 1337 are now
        # capped at 2 threads, all other MYREFERRER requests still capped at 20 threads
    """

    def __init__(self, project_field: str):
        self.__project_field = project_field

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        enabled = get_config(ENABLED_CONFIG, 1)
        if not enabled:
            return

        referrer_thread_quota = get_config(
            f"{REFERRER_CONFIG}_{query_settings.referrer}"
        )
        project_ids = get_object_ids_in_query_ast(query, self.__project_field)
        if not project_ids and not referrer_thread_quota:
            return

        # TODO: Like for the rate limiter Add logic for multiple IDs
        project_id = str(project_ids.pop()) if project_ids else "NO_PROJECT_ID"
        project_referrer_thread_quota = get_config(
            f"{REFERRER_PROJECT_CONFIG}_{query_settings.referrer}_{project_id}"
        )
        thread_quota = project_referrer_thread_quota or referrer_thread_quota
        if not thread_quota:
            return

        assert isinstance(thread_quota, int)
        query_settings.set_resource_quota(ResourceQuota(max_threads=thread_quota))
