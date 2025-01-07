from __future__ import annotations

from enum import Enum

from snuba.admin.clickhouse.common import PreDefinedQuery
from snuba.utils.registered_class import RegisteredClass

# Start index is 9223372036854775808
METRICS_START_INDEX = 1 << 63


# TODO: These enum definitions do not work right now. Most likely because
# f-string formatting does not work as intended when defining the sql. We hardcode
# the values in the queries for now.
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

    sql = """
    SELECT org_id, project_id,
    tags.raw_value [indexOf(tags.key, 9223372036854776062)] AS `span.category`,
    uniq(tags.raw_value [indexOf(tags.key, 9223372036854776060)]) AS count_groups
    FROM generic_metric_distributions_aggregated_dist
    WHERE (granularity = 2)
    AND (timestamp >= now() - INTERVAL {{hour_window}} HOUR)
    AND (metric_id IN [9223372036854776212, 9223372036854776213])
    AND `span.category` = '{{span_category}}'
    GROUP BY org_id, project_id, `span.category`
    ORDER BY count_groups DESC
    LIMIT 100
    """


class SpanGroupingCardinalitySamples(CardinalityQuery):
    """
    Get a sample list of the span groups for a given org and project.
    """

    sql = """
    SELECT DISTINCT
    tags.raw_value[indexOf(tags.key, 9223372036854776062)] AS `span.category`,
    tags.raw_value[indexOf(tags.key, 9223372036854776060)] AS `span.group`,
    tags.raw_value[indexOf(tags.key, 9223372036854776057)] AS `span.description`
    FROM generic_metric_distributions_aggregated_dist
    WHERE (granularity = 2)
    AND (timestamp >= now() - INTERVAL {{hour_window}} HOUR)
    AND (org_id = {{org_id}})
    AND (project_id = {{project_id}})
    AND (metric_id = 9223372036854776214)
    ORDER BY span.description
    LIMIT 1000
    """


class BucketsByOrgProject(CardinalityQuery):
    """
    For a given org/project, find out how many buckets are generated for each granularity.
    The query asks for which metric type (counters/sets/distributions), the hour window to look back, and the org/project ID.

    Note that gauges uses a different timestamp (rounded_timestamp).

    This also has a totals row at the end.
    """

    sql = """
    SELECT org_id, project_id, granularity, count()
    FROM generic_metric_{{metric_type}}_aggregated_dist
    WHERE timestamp >= (now() - INTERVAL {{hour_window}} HOUR)
    AND org_id = {{org_id}}
    AND project_id = {{project_id}}
    GROUP BY org_id, project_id, granularity
    WITH TOTALS
    ORDER BY granularity ASC
    """


class BucketsByOrgProjectGauges(CardinalityQuery):
    """
    For a given org/project, find out how many buckets are generated for each granularity.
    This query is for gauges specifically, since it uses timestamps differently.
    The query asks for the hour window to look back, and the org/project ID.

    This also has a totals row at the end.
    """

    sql = """
    SELECT org_id, project_id, granularity, count()
    FROM generic_metric_gauges_aggregated_dist
    WHERE rounded_timestamp >= (now() - INTERVAL {{hour_window}} HOUR)
    AND org_id = {{org_id}}
    AND project_id = {{project_id}}
    GROUP BY org_id, project_id, granularity
    WITH TOTALS
    ORDER BY granularity ASC
    """


class DatapointsByOrgProjectDistributions(CardinalityQuery):
    """
    For a given org/project, find out how many datapoints exist for each granularity.
    """

    sql = """
    SELECT org_id, project_id, granularity, countMerge(count)
    FROM generic_metric_distributions_aggregated_dist
    WHERE timestamp >= (now() - INTERVAL {{hour_window}} HOUR)
    AND org_id = {{org_id}}
    AND project_id = {{project_id}}
    GROUP BY org_id, project_id, granularity
    WITH TOTALS
    ORDER BY granularity ASC
    """


class DatapointsByOrgProjectCounters(CardinalityQuery):
    """
    For a given org/project, find out how many datapoints exist for each granularity.
    """

    sql = """
    SELECT org_id, project_id, granularity, sumMerge(value)
    FROM generic_metric_counters_aggregated_dist
    WHERE timestamp >= (now() - INTERVAL {{hour_window}} HOUR)
    AND org_id = {{org_id}}
    AND project_id = {{project_id}}
    GROUP BY org_id, project_id, granularity
    WITH TOTALS
    ORDER BY granularity ASC
    """


class DatapointsByOrgProjectGauges(CardinalityQuery):
    """
    For a given org/project, find out how many datapoints exist for each granularity.
    """

    sql = """
    SELECT org_id, project_id, granularity, sumMerge(count)
    FROM generic_metric_gauges_aggregated_dist
    WHERE rounded_timestamp >= (now() - INTERVAL {{hour_window}} HOUR)
    AND org_id = {{org_id}}
    AND project_id = {{project_id}}
    GROUP BY org_id, project_id, granularity
    WITH TOTALS
    ORDER BY granularity ASC
    """


class BucketsPerOrgOverTime(CardinalityQuery):
    """
    Determine how many buckets an org is storing over time. Show the top 5 offenders.

    By default, shows custom metrics. You can adjust the query for metric platform use cases including:
    spans, tranactions, sessions, escalating_issues, profiles, bundle_analysis, and custom.

    This query can be tweaked by using more/less granular time function (toStartOfTenMinutes)
    or by using project_id instead of org_id.
    """

    sql = """
    SELECT toStartOfHour(timestamp) as time, org_id, count() as total
    FROM generic_metric_{{metric_type}}_aggregated_dist
    WHERE timestamp >= (now() - INTERVAL {{hour}} HOUR)
    AND use_case_id  = 'custom'
    GROUP BY time, org_id
    ORDER BY time ASC, total DESC
    LIMIT 5 BY time
    """
