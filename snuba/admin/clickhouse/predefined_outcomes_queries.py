from __future__ import annotations

from snuba.admin.clickhouse.common import PreDefinedQuery
from snuba.utils.registered_class import RegisteredClass


# Every line of a `sql` body below must start with at least the 4 spaces of class
# indentation: the frontend de-indents predefined queries with
# `line.substring(4)` before putting them in the editor. Indentation *beyond*
# those 4 spaces is preserved.
#
# Common DataCategory values (from Relay):
#   1 = error, 2 = transaction, 3 = security, 4 = attachment,
#   5 = default, 6 = session, 7 = replay, 8 = profile,
#   9 = profile_chunk, 10 = span, 11 = monitor, 21 = log_item,
#   22 = attachment_item (attachment count), 23 = uptime
# Outcome values:
#   0 = accepted, 1 = filtered, 2 = rate_limited, 3 = invalid,
#   4 = abuse, 5 = client_discard
class OutcomesQuery(PreDefinedQuery, metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


class VolumeByCategoryOverTime(OutcomesQuery):
    """
    Total quantity per hour for a data category over a lookback window.
    Useful for spotting sudden volume spikes (e.g. replays category=7,
    attachment bytes category=4, attachment counts category=22).
    """

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY hour
    ORDER BY hour DESC
    """


class TopOrgsByCategory(OutcomesQuery):
    """
    Top organizations by quantity for a data category in a lookback window.
    Each row is one org/hour pair, ordered by volume. Use this to find which
    orgs are driving a spike.
    """

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY hour, org_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """


class TopOrgsByCategoryAggregated(OutcomesQuery):
    """
    Top organizations by total quantity for a data category, aggregated across
    the whole lookback window (not broken out by hour).
    """

    sql = """
    SELECT
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY org_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """


class OrgVolumeOverTime(OutcomesQuery):
    """
    Quantity per hour for a specific org and data category. Drill into a noisy
    org found via the top-orgs queries.
    """

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND org_id = {{org_id}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY hour, org_id
    ORDER BY hour DESC
    """


class OrgVolumeByReason(OutcomesQuery):
    """
    Quantity per hour broken down by reason for a specific org and category.
    Reveals whether volume is accepted, filtered (e.g. network_error,
    web-crawlers), rate limited (replay_usage_exceeded), or abuse-limited
    (project_abuse_limit).
    """

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        org_id,
        reason,
        outcome,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND org_id = {{org_id}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY hour, org_id, reason, outcome
    ORDER BY hour DESC, total_quantity DESC
    """


class OrgVolumeByProject(OutcomesQuery):
    """
    Quantity per project for a specific org and category over a lookback window.
    Helps isolate which project inside an org is responsible for volume.
    """

    sql = """
    SELECT
        project_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND org_id = {{org_id}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY project_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """


class VolumeByOutcomeOverTime(OutcomesQuery):
    """
    Quantity per hour broken down by outcome code for a data category.
    Outcome: 0=accepted, 1=filtered, 2=rate_limited, 3=invalid, 4=abuse,
    5=client_discard.
    """

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        outcome,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= now() - INTERVAL {{lookback_hours}} HOUR
    GROUP BY hour, outcome
    ORDER BY hour DESC, total_quantity DESC
    """


class TimeRangeTopOrgs(OutcomesQuery):
    """
    Top orgs for a category inside an explicit time range
    (from_ts / to_ts as 'YYYY-MM-DD HH:MM:SS'). Use when investigating a
    known incident window rather than a relative lookback.
    """

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= '{{from_ts}}'
        AND timestamp < '{{to_ts}}'
    GROUP BY hour, org_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """
