from __future__ import annotations

from enum import Enum

from snuba.admin.clickhouse.common import PreDefinedQuery
from snuba.utils.registered_class import RegisteredClass

# Start index is 9223372036854775808
METRICS_START_INDEX = 1 << 63


class IndexedIDs(Enum):
    # Tags
    SPAN_DESCRIPTION_TAG = METRICS_START_INDEX + 249
    SPAN_CATEGORY_TAG = METRICS_START_INDEX + 254
    SPAN_GROUP_TAG = METRICS_START_INDEX + 252
    # Metric names
    SPAN_DURATION_METRIC = METRICS_START_INDEX + 404
    SPAN_EXCLUSIVE_TIME_METRIC = METRICS_START_INDEX + 405
    SPAN_EXCLUSIVE_TIME_LIGHT_METRIC = METRICS_START_INDEX + 406


class CardinalityQuery(PreDefinedQuery, metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


class SpanGroupingCardinality(CardinalityQuery):
    """Get the span groups with the highest cardinality across all projects and orgs."""

    sql = f"""
SELECT org_id, project_id,
    tags.raw_value [indexOf(tags.key, {IndexedIDs.SPAN_CATEGORY_TAG})] AS `span.category`,
    uniq(tags.raw_value [indexOf(tags.key, {IndexedIDs.SPAN_GROUP_TAG})] AS `span group`) AS groups
FROM generic_metric_distributions_aggregated_dist
WHERE (granularity = 3)
AND (timestamp >= now() - INTERVAL {{hour_window}} HOUR)
AND (metric_id IN [{IndexedIDs.SPAN_DURATION_METRIC}, {IndexedIDs.SPAN_EXCLUSIVE_TIME_METRIC}])
AND `span.category` = {{span_category}}
GROUP BY org_id, project_id, `span.category`
ORDER BY groups DESC
LIMIT 100
"""


class SpanGroupingCardinalitySamples(CardinalityQuery):
    """
    Get a sample list of the span groups for a given org and project.
    """

    sql = f"""
SELECT DISTINCT
    tags.raw_value[indexOf(tags.key, {IndexedIDs.SPAN_CATEGORY_TAG})] AS `span.category`,
    tags.raw_value[indexOf(tags.key, {IndexedIDs.SPAN_GROUP_TAG})] AS `span.group`,
    tags.raw_value[indexOf(tags.key, {IndexedIDs.SPAN_DESCRIPTION_TAG})] AS `span.description`
FROM generic_metric_distributions_aggregated_dist
WHERE (granularity = 3)
AND (timestamp >= now() - INTERVAL {{hour_window}} HOUR)
AND (org_id = {{org_id}})
AND (project_id = {{project_id}})
AND (metric_id = {IndexedIDs.SPAN_EXCLUSIVE_TIME_LIGHT_METRIC})
ORDER BY span.description
LIMIT 1000
"""
