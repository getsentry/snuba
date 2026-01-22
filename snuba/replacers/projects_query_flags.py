from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

import sentry_sdk
from redis.cluster import ClusterPipeline as StrictClusterPipeline

from snuba import settings
from snuba.processor import ReplacementType
from snuba.redis import RedisClientKey, get_redis_client
from snuba.replacers.replacer_processor import ReplacerState
from snuba.state import get_config

redis_client = get_redis_client(RedisClientKey.REPLACEMENTS_STORE)


@dataclass
class ProjectsQueryFlags:
    """
    These flags are useful for ensuring a Query does not look at certain replaced
    data. They are also useful for knowing whether or not to set the Query as
    FINAL overall.

    - needs_final: Whether or not any project was set as final.
    - group_ids_to_exclude: A set of group id's that have been replaced, and
    the replacement has not yet been merged in the database. These groups should be
    excluded from the data a Query looks through.
    - replacement_types: A set of all replacement types across replacements for the
    set of project ids.
    - latest_replacement_time: The latest timestamp any replacement occured.
    """

    needs_final: bool
    group_ids_to_exclude: Set[int]
    replacement_types: Set[str]
    latest_replacement_time: Optional[datetime]

    @staticmethod
    def set_project_needs_final(
        project_id: int,
        state_name: Optional[ReplacerState],
        replacement_type: ReplacementType,
    ) -> None:
        key, type_key = ProjectsQueryFlags._build_project_needs_final_key_and_type_key(
            project_id, state_name
        )
        with redis_client.pipeline() as p:
            p.set(key, time.time(), ex=settings.REPLACER_KEY_TTL)
            p.set(type_key, replacement_type, ex=settings.REPLACER_KEY_TTL)
            p.execute()

    @staticmethod
    def set_project_exclude_groups(
        project_id: int,
        group_ids: Sequence[int],
        state_name: Optional[ReplacerState],
        #  replacement type is just for metrics, not necessary for functionality
        replacement_type: ReplacementType,
    ) -> None:
        """
        This method is called when a replacement comes in. For a specific project, record
        the group ids which were deleted as a result of this replacement

        Add {group_id: now, ...} to the ZSET for each `group_id` to exclude,
        remove outdated entries based on `settings.REPLACER_KEY_TTL`, and expire
        the entire ZSET incase it's rarely touched.

        Add replacement type for this replacement.
        """
        now = time.time()
        (
            key,
            type_key,
        ) = ProjectsQueryFlags._build_project_exclude_groups_key_and_type_key(
            project_id, state_name
        )
        with redis_client.pipeline() as p:
            # the redis key size limit is defined as 2 times the clickhouse query size
            # limit. there is an explicit check in the query processor for the same
            # limit
            max_group_ids_exclude = get_config(
                "max_group_ids_exclude",
                settings.REPLACER_MAX_GROUP_IDS_TO_EXCLUDE,
            )
            assert isinstance(max_group_ids_exclude, int)

            group_id_data: MutableMapping[str | bytes, bytes | float | int | str] = {}
            for group_id in group_ids:
                group_id_data[str(group_id)] = now
                if len(group_id_data) > 2 * max_group_ids_exclude:
                    break

            p.zadd(key, group_id_data)
            ProjectsQueryFlags._truncate_group_id_replacement_set(
                p, key, now, max_group_ids_exclude
            )
            p.expire(key, int(settings.REPLACER_KEY_TTL))

            # store the replacement type data
            replacement_type_data: Mapping[str | bytes, bytes | float | int | str] = {
                replacement_type: now
            }
            p.zadd(type_key, replacement_type_data)
            ProjectsQueryFlags._truncate_group_id_replacement_set(
                p, type_key, now, max_group_ids_exclude
            )
            p.expire(type_key, int(settings.REPLACER_KEY_TTL))

            p.execute()

    @classmethod
    def load_from_redis(
        cls, project_ids: Sequence[int], state_name: Optional[ReplacerState]
    ) -> ProjectsQueryFlags:
        """
        Loads flags for given project ids.

        - Searches through Redis for relevant replacements info
        - Splits up results from pipeline into something that makes sense

        Fails open, in case redis is unavailable, query goes through
        """
        s_project_ids = set(project_ids)

        try:
            with redis_client.pipeline() as p:
                with sentry_sdk.start_span(op="function", description="build_redis_pipeline"):
                    cls._query_redis(s_project_ids, state_name, p)

                with sentry_sdk.start_span(
                    op="function", description="execute_redis_pipeline"
                ) as span:
                    results = p.execute()
                    # getting size of str(results) since sys.getsizeof() doesn't count recursively
                    span.set_tag("results_size", sys.getsizeof(str(results)))

            with sentry_sdk.start_span(op="function", description="process_redis_results") as span:
                flags = cls._process_redis_results(results, len(s_project_ids))
                span.set_tag("projects", s_project_ids)
                span.set_tag("exclude_groups", flags.group_ids_to_exclude)
                span.set_tag("len(exclude_groups)", len(flags.group_ids_to_exclude))
                span.set_tag("latest_replacement_time", flags.latest_replacement_time)
                span.set_tag("replacement_types", flags.replacement_types)

            return flags
        except Exception as e:
            sentry_sdk.capture_exception(e)
            return cls(
                needs_final=False,
                group_ids_to_exclude=set([]),
                replacement_types=set([]),
                latest_replacement_time=None,
            )

    @classmethod
    def _process_redis_results(cls, results: List[Any], len_projects: int) -> ProjectsQueryFlags:
        """
        Produces readable data from flattened list of Redis pipeline results.

        `results` is a flat list of all the redis call results of _query_redis
        [
            needs_final: Sequence[timestamp]...,
            _: Sequence[num_removed_elements]...,
            exclude_groups: Sequence[List[group_id]]...,
            needs_final_replacement_types: Sequence[Optional[str]]...,
            _: Sequence[num_removed_elements]...,
            groups_replacement_types: Sequence[List[str]]...,
            latest_exclude_groups_replacements: Sequence[Optional[Tuple[group_id, datetime]]]...
        ]
        - The _ slices are the results of `zremrangebyscore` calls, unecessary data
        - Since the Redis commands are built to result in something per project per command,
        the results can be split up with multiples of `len_projects` as indices
        """
        needs_final_result = results[:len_projects]
        exclude_groups_results = results[len_projects * 2 : len_projects * 3]
        projects_replacment_types_result = results[len_projects * 3 : len_projects * 4]
        groups_replacement_types_results = results[len_projects * 5 : len_projects * 6]
        latest_exclude_groups_result = results[len_projects * 6 : len_projects * 7]

        needs_final = any(needs_final_result)

        exclude_groups = {
            int(group_id)
            for exclude_groups_result in exclude_groups_results
            for group_id in exclude_groups_result
        }

        needs_final_replacement_types = {
            replacement_type.decode("utf-8")
            for replacement_type in projects_replacment_types_result
            if replacement_type
        }

        groups_replacement_types = {
            replacement_type.decode("utf-8")
            for groups_replacement_types_result in groups_replacement_types_results
            for replacement_type in groups_replacement_types_result
        }

        replacement_types = groups_replacement_types.union(needs_final_replacement_types)

        latest_replacement_time = cls._process_latest_replacement(
            needs_final, needs_final_result, latest_exclude_groups_result
        )

        flags = cls(needs_final, exclude_groups, replacement_types, latest_replacement_time)
        return flags

    @staticmethod
    def _query_redis(
        project_ids: Set[int],
        state_name: Optional[ReplacerState],
        p: StrictClusterPipeline,
    ) -> None:
        """
        Builds Redis calls in the pipeline p to get all necessary replacements
        data for the given set of project ids.

        All queried data has been previously set in setter functions
        above this class.
        """
        needs_final_keys_and_type_keys = [
            ProjectsQueryFlags._build_project_needs_final_key_and_type_key(project_id, state_name)
            for project_id in project_ids
        ]

        for needs_final_key, _ in needs_final_keys_and_type_keys:
            p.get(needs_final_key)

        exclude_groups_keys_and_types = [
            ProjectsQueryFlags._build_project_exclude_groups_key_and_type_key(
                project_id, state_name
            )
            for project_id in project_ids
        ]

        ProjectsQueryFlags._remove_stale_and_load_new_sorted_set_data(
            p,
            [groups_key for groups_key, _ in exclude_groups_keys_and_types],
        )

        for _, needs_final_type_key in needs_final_keys_and_type_keys:
            p.get(needs_final_type_key)

        ProjectsQueryFlags._remove_stale_and_load_new_sorted_set_data(
            p, [type_key for _, type_key in exclude_groups_keys_and_types]
        )

        # retrieve the latest timestamp for any exclude groups replacement
        for exclude_groups_key, _ in exclude_groups_keys_and_types:
            p.zrevrange(
                exclude_groups_key,
                0,
                0,
                withscores=True,
            )

    @staticmethod
    def _remove_stale_and_load_new_sorted_set_data(
        p: StrictClusterPipeline, keys: List[str]
    ) -> None:
        """
        Remove stale data per key according to TTL.
        Get latest data per key.

        Split across two loops to avoid intertwining Redis calls and
        consequentially, their results.
        """
        now = time.time()

        for key in keys:
            p.zremrangebyscore(key, float("-inf"), now - settings.REPLACER_KEY_TTL)
        for key in keys:
            p.zrevrangebyscore(key, float("inf"), now - settings.REPLACER_KEY_TTL)

    @staticmethod
    def _process_latest_replacement(
        needs_final: bool,
        needs_final_result: List[Any],
        latest_exclude_groups_result: List[Any],
    ) -> Optional[datetime]:
        """
        Process the relevant replacements data to look for the latest timestamp
        any replacement occured.
        """
        latest_replacements = set()
        if needs_final:
            latest_need_final_replacement_times = [
                # Backwards compatibility: Before it was simply "True" at each key,
                # now it's the timestamp at which the key was added.
                float(timestamp)
                for timestamp in needs_final_result
                if timestamp and timestamp != b"True"
            ]
            if latest_need_final_replacement_times:
                latest_replacements.add(max(latest_need_final_replacement_times))

        for latest_exclude_groups in latest_exclude_groups_result:
            if latest_exclude_groups:
                [(_, timestamp)] = latest_exclude_groups
                latest_replacements.add(timestamp)

        return datetime.fromtimestamp(max(latest_replacements)) if latest_replacements else None

    @staticmethod
    def _build_project_needs_final_key_and_type_key(
        project_id: int, state_name: Optional[ReplacerState]
    ) -> Tuple[str, str]:
        key = f"project_needs_final:{f'{state_name.value}:' if state_name else ''}{project_id}"
        return key, f"{key}-type"

    @staticmethod
    def _build_project_exclude_groups_key_and_type_key(
        project_id: int, state_name: Optional[ReplacerState]
    ) -> Tuple[str, str]:
        key = f"project_exclude_groups:{f'{state_name.value}:' if state_name else ''}{project_id}"
        return key, f"{key}-type"

    @staticmethod
    def _truncate_group_id_replacement_set(
        p: StrictClusterPipeline, key: str, now: float, max_group_ids_exclude: int
    ) -> None:
        # remove group id deletions that should have been merged by now
        p.zremrangebyscore(key, -1, now - settings.REPLACER_KEY_TTL)

        # remove group id deletions that exceed the maximum number of deletions
        # snuba's query processor will put in a query.
        #
        # Add +2 because:
        #
        # - ranges are exclusive in redis (apparently)
        # - such that the query processor will recognize that we exceeded the limit
        #   and fall back to FINAL (because the set doesn't contain all group ids to
        #   exclude anymore)
        p.zremrangebyrank(key, 0, -(2 * max_group_ids_exclude + 2))
