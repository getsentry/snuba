from dataclasses import replace
from datetime import datetime
from typing import MutableMapping, Optional, Set

from snuba import environment, settings
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import (
    get_object_ids_in_query_ast,
    get_time_range,
)
from snuba.query.conditions import not_in_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings, SubscriptionQuerySettings
from snuba.replacers.projects_query_flags import ProjectsQueryFlags
from snuba.replacers.replacer_processor import ReplacerState
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "processors.replaced_groups")
FINAL_METRIC = "final"
CONSISTENCY_DENYLIST_METRIC = "post_replacement_consistency_projects_denied"


class PostReplacementConsistencyEnforcer(ClickhouseQueryProcessor):
    """
    This processor tweaks the query to ensure that groups that have been manipulated
    by a replacer (like after a deletion) are excluded if they need to be.

    There is a period of time between the replacement executing its query and Clickhouse
    merging the rows to achieve consistency. During this period of time we either
    have to remove those rows manually or to run the query in FINAL mode.
    """

    def __init__(self, project_column: str, replacer_state_name: Optional[str]) -> None:
        self.__project_column = project_column
        self.__groups_column = "group_id"
        # This is used to allow us to keep the replacement state in redis for multiple
        # replacers on multiple tables. replacer_state_name is part of the redis key.
        self.__replacer_state_name = (
            ReplacerState(replacer_state_name) if replacer_state_name else None
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        if query_settings.get_turbo():
            return

        project_ids = get_object_ids_in_query_ast(query, self.__project_column)

        if project_ids is None:
            self._set_query_final(query, False)
            return

        for no_final_subscriptions_project in (
            get_config("skip_final_subscriptions_projects") or "[]"
        )[1:-1].split(","):
            if (
                no_final_subscriptions_project
                and int(no_final_subscriptions_project) in project_ids
                and isinstance(query_settings, SubscriptionQuerySettings)
            ):
                metrics.increment(name="subscriptions_skipped_final")
                self._set_query_final(query, False)
                return

        for denied_project_id_string in (
            get_config("post_replacement_consistency_projects_denylist") or "[]"
        )[1:-1].split(","):
            if (
                denied_project_id_string
                and int(denied_project_id_string) in project_ids
            ):
                metrics.increment(name=CONSISTENCY_DENYLIST_METRIC)
                self._set_query_final(query, True)
                return

        flags: ProjectsQueryFlags = ProjectsQueryFlags.load_from_redis(
            list(project_ids), self.__replacer_state_name
        )

        query_overlaps_replacement = self._query_overlaps_replacements(
            query, flags.latest_replacement_time
        )

        if not query_overlaps_replacement:
            self._set_query_final(query, False)
            return

        tags = self._initialize_tags(query_settings, flags)
        set_final = False

        if flags.needs_final:
            if len(flags.replacement_types) == 1:
                tags["cause"] = "flag_{next(iter(flags.replacement_types))}"
            else:
                tags["cause"] = "flag_multiple"
            metrics.increment(
                name=FINAL_METRIC,
                tags=tags,
            )
            set_final = True
        elif flags.group_ids_to_exclude:
            # If the number of groups to exclude exceeds our limit, the query
            # should just use final instead of the exclusion set.
            max_group_ids_exclude = get_config(
                "max_group_ids_exclude",
                settings.REPLACER_MAX_GROUP_IDS_TO_EXCLUDE,
            )
            assert isinstance(max_group_ids_exclude, int)
            groups_to_exclude = self._groups_to_exclude(
                query, flags.group_ids_to_exclude
            )
            if (
                len(flags.group_ids_to_exclude) > 2 * max_group_ids_exclude
                or len(groups_to_exclude) > max_group_ids_exclude
            ):
                tags["cause"] = "max_groups"
                metrics.increment(
                    name=FINAL_METRIC,
                    tags=tags,
                )
                set_final = True
            elif groups_to_exclude:
                query.add_condition_to_ast(
                    not_in_condition(
                        FunctionCall(
                            None,
                            "assumeNotNull",
                            (Column(None, None, self.__groups_column),),
                        ),
                        [Literal(None, p) for p in groups_to_exclude],
                    )
                )

        self._set_query_final(query, set_final)

    def _initialize_tags(
        self, query_settings: QuerySettings, flags: ProjectsQueryFlags
    ) -> MutableMapping[str, str]:
        """
        Initialize tags dictionary for DataDog metrics.
        """
        tags = {
            replacement_type: "True" for replacement_type in flags.replacement_types
        }
        tags["referrer"] = query_settings.referrer
        return tags

    def _set_query_final(self, query: Query, final: bool) -> None:
        """
        Set the 'final' clause of a Query.
        A query set as final will force ClickHouse to perform a merge
        on the results of the query. This is very performance heavy and
        should be avoided whenever possible.
        """
        query.set_from_clause(replace(query.get_from_clause(), final=final))

    def _query_overlaps_replacements(
        self,
        query: Query,
        latest_replacement_time: Optional[datetime],
    ) -> bool:
        """
        Given a Query and the latest replacement time for any project
        this query touches, returns whether or not this Query's time
        range overlaps that replacement.
        """
        query_from, _ = get_time_range(query, "timestamp")
        return (
            latest_replacement_time > query_from
            if latest_replacement_time and query_from
            else True
        )

    def _groups_to_exclude(
        self, query: Query, group_ids_to_exclude: Set[int]
    ) -> Set[int]:
        """
        Given a Query and the group ids to exclude for any project
        this query touches, returns the intersection of the group ids
        from the replacements and the group ids this Query explicitly
        queries for, if any.

        Eg.
        - The query specifically looks for group ids: {1, 2}
        - The replacements on the projects require exclusion of groups: {1, 3, 4}
        - The query only needs to exclude group id 1
        """

        groups_in_query = get_object_ids_in_query_ast(query, self.__groups_column)

        if groups_in_query:
            group_ids_to_exclude = group_ids_to_exclude.intersection(groups_in_query)

        return group_ids_to_exclude
