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
            agg = check_aggregate(data, expected_data, "aggregate")
            if agg is not None:
                return agg

        if referrer == "tagstore.get_groups_user_counts":
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
