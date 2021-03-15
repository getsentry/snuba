from typing import Any, Mapping, Optional, Sequence


def assign_reason_category(
    data: Sequence[Mapping[str, Any]],
    expected_data: Sequence[Mapping[str, Any]],
    referrer: str,
) -> str:
    """
    Attempt to categorize the reason for the discrepancy.
    Mostly based on the type of query we expect for each referrer.
    """
    try:
        if referrer.startswith("tsdb-modelid:"):
            missing_data = check_missing_data(data, expected_data)
            if missing_data is not None:
                return missing_data

            out_of_order = check_result_out_of_order(data, expected_data)
            if out_of_order is not None:
                return out_of_order

            agg = check_aggregate(data, expected_data, "aggregate")
            if agg is not None:
                return agg

        if referrer == "tagstore.get_groups_user_counts":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, ["count"]
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer == "api.serializer.projects.get_stats":
            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer == "tagstore.__get_tag_key_and_top_values":
            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer == "tagstore.__get_tag_keys":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, ["count"]
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

        if referrer == "api.organization-events-facets.top-tags":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, ["count", "tags_key"]
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer == "tagstore.get_tag_value_paginator_for_projects":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, ["last_seen"]
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

        if referrer == "tagstore.__get_tag_keys_and_top_values":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, ["count"]
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

        if referrer == "tagstore.get_release_tags":
            agg = check_aggregate(data, expected_data, "times_seen")
            if agg is not None:
                return agg

        if referrer == "api.organization-events-meta":
            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer in [
            "api.organization-event-stats",
            "api.organization-event-stats.find-topn",
        ]:
            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer == "serializers.GroupSerializerSnuba._execute_seen_stats_query":
            agg = check_aggregate(data, expected_data, "times_seen")
            if agg is not None:
                return agg

        if referrer == "api.organization-sdk-updates":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, ["project"]
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

        if referrer == "subscription":
            agg = check_aggregate(data, expected_data, "count")
            if agg is not None:
                return agg

        if referrer == "serializers.GroupSerializerSnuba._execute_seen_stats_query":
            nondeterministic_query = check_nondeterministic_query(
                data, expected_data, []
            )
            if nondeterministic_query is not None:
                return nondeterministic_query

        return "UNKNOWN"

    except Exception:
        return "UNKNOWN"


def check_aggregate(
    data: Sequence[Mapping[str, Any]],
    expected_data: Sequence[Mapping[str, Any]],
    aggregate_key: str,
) -> Optional[str]:
    """
    Returns AGGREGATE_TOO_HIGH or AGGREGATE_TOO_LOW if the result is the same
    except for the aggregate value. Could be an indication of differences in how
    parts are merged between the two tables, timing differences or something else.
    """
    if len(data) == len(expected_data):
        for idx in range(len(data)):
            if data[idx] != expected_data[idx]:
                non_matching_keys = set()
                for key in data[idx]:
                    if data[idx][key] != expected_data[idx][key]:
                        non_matching_keys.add(key)

                if non_matching_keys == {aggregate_key}:
                    if data[idx][aggregate_key] > expected_data[idx][aggregate_key]:
                        return "AGGREGATE_TOO_HIGH"
                    else:
                        return "AGGREGATE_TOO_LOW"

    return None


def check_missing_data(
    data: Sequence[Mapping[str, Any]], expected_data: Sequence[Mapping[str, Any]]
) -> Optional[str]:
    """
    Returns FOUND_EXTRA_DATA if extra rows as found, returns FOUND_MISSING_DATA if
    rows are missing.
    """
    if len(data) > len(expected_data):
        if all(row in data for row in expected_data):
            return "FOUND_EXTRA_DATA"
    if len(data) < len(expected_data):
        if all(row in expected_data for row in data):
            return "FOUND_MISSING_DATA"

    return None


def check_result_out_of_order(
    data: Sequence[Mapping[str, Any]], expected_data: Sequence[Mapping[str, Any]],
) -> Optional[str]:
    """
    Returns RESULT_OUT_OF_ORDER if the entries are the same but they are present
    in a different order. Could be an indication of a missing order by term in the query
    or a column with a different type in both tables causing different ordering.
    """
    if len(data) == len(expected_data):
        if all(row in data for row in expected_data):
            return "RESULT_OUT_OF_ORDER"

    return None


def check_nondeterministic_query(
    data: Sequence[Mapping[str, Any]],
    expected_data: Sequence[Mapping[str, Any]],
    sort_fields: Sequence[str],
) -> Optional[str]:
    """
    Returns NONDETERMINISTIC_QUERY if different rows are returned due
    to a missing or non unique order by clause.
    """
    if len(data) == len(expected_data):
        for idx in range(len(data)):
            if data[idx] != expected_data[idx]:
                if all(
                    data[idx][col] == expected_data[idx][col]
                    for col in data[idx]
                    if col in sort_fields
                ):
                    return "NONDETERMINISTIC_QUERY"

    return None
