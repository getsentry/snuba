from enum import Enum
from typing import NamedTuple, NewType

# Start index is 9223372036854775808
METRICS_START_INDEX = 1 << 63
CARDINALITY_LIMIT = 5000

SQL = NewType("SQL", str)


class IndexedIDs(Enum):
    # Tags
    SPAN_DESCRIPTION_TAG = METRICS_START_INDEX + 249
    SPAN_CATEGORY_TAG = METRICS_START_INDEX + 254
    SPAN_GROUP_TAG = METRICS_START_INDEX + 252
    # Metric names
    SPAN_DURATION_METRIC = METRICS_START_INDEX + 404
    SPAN_EXCLUSIVE_TIME_METRIC = METRICS_START_INDEX + 405
    SPAN_EXCLUSIVE_TIME_LIGHT_METRIC = METRICS_START_INDEX + 406


class SpanGroupingCardinalityResult(NamedTuple):
    org_id: int
    project_id: int
    category: str
    groups: int


def span_grouping_distinct_modules_query(time_window_hrs: int) -> SQL:
    """
    Form the clickhouse query to get the distinct span modules we are ingesting.
    The result of this query can be used to get the cardinality of individual modules.
    """
    if time_window_hrs > 3:
        raise ValueError("Time window cannot be greater than 2 hours")

    query = f"""
SELECT DISTINCT
tags.raw_value [indexOf(tags.key, {IndexedIDs.SPAN_CATEGORY_TAG.value})] AS `span.category`
FROM generic_metric_distributions_aggregated_dist
WHERE granularity = 2
AND timestamp >= now() - INTERVAL {time_window_hrs} HOUR
AND metric_id IN [{IndexedIDs.SPAN_DURATION_METRIC.value}, {IndexedIDs.SPAN_EXCLUSIVE_TIME_METRIC.value}]
"""
    return SQL(query)


def span_grouping_cardinality_query(
    span_category: str, time_window_hrs: int, limit: int
) -> SQL:
    """
    Form the clickhouse query to analyze the cardinality of metrics extracted from spans.
    The result of this query informs how high the cardinality of a given span module is.
    """
    if time_window_hrs > 24:
        raise ValueError("Time window cannot be greater than 24 hours")

    if limit > 1000:
        raise ValueError("Limit cannot be greater than 1000")

    query = f"""
SELECT org_id, project_id,
tags.raw_value [indexOf(tags.key, {IndexedIDs.SPAN_CATEGORY_TAG.value})] AS `span.category`,
uniq(tags.raw_value [indexOf(tags.key, {IndexedIDs.SPAN_GROUP_TAG.value})]) AS count_groups
FROM generic_metric_distributions_aggregated_dist
WHERE (granularity = 2)
AND (timestamp >= now() - INTERVAL {time_window_hrs} HOUR)
AND (metric_id IN [{IndexedIDs.SPAN_DURATION_METRIC.value},
{IndexedIDs.SPAN_EXCLUSIVE_TIME_METRIC.value}])
AND `span.category` = '{span_category}'
GROUP BY org_id, project_id, `span.category`
HAVING count_groups > {CARDINALITY_LIMIT}
ORDER BY count_groups DESC
LIMIT {limit}
"""
    return SQL(query)


def span_samples_for_group_query(
    span_group: SpanGroupingCardinalityResult, time_window_hrs: int, limit: int
) -> SQL:
    """
    Form the query to get the sample of spans for a given span group.
    """
    if limit > 2000:
        raise ValueError("Limit cannot be greater than 2000")

    query = f"""
SELECT DISTINCT
tags.raw_value[indexOf(tags.key, {IndexedIDs.SPAN_CATEGORY_TAG.value})] AS `span.op`,
tags.raw_value[indexOf(tags.key, {IndexedIDs.SPAN_GROUP_TAG.value})] AS `span.group`,
tags.raw_value[indexOf(tags.key, {IndexedIDs.SPAN_DESCRIPTION_TAG.value})] AS `span.description`
FROM generic_metric_distributions_aggregated_dist
WHERE (granularity = 2)
AND (timestamp >= now() - INTERVAL {time_window_hrs} HOUR)
AND (org_id = {span_group.org_id})
AND (project_id = {span_group.project_id})
AND (metric_id = {IndexedIDs.SPAN_EXCLUSIVE_TIME_LIGHT_METRIC.value})
ORDER BY `span.description`
LIMIT {limit}
"""
    return SQL(query)
