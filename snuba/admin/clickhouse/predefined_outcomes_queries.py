from __future__ import annotations

from snuba.admin.clickhouse.common import PreDefinedQuery, format_predefined_sql
from snuba.utils.registered_class import RegisteredClass


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

    @classmethod
    def to_json(cls) -> dict[str, str]:
        payload = super().to_json()
        payload["sql"] = format_predefined_sql(cls.sql)
        return payload


class VolumeByCategoryOverTime(OutcomesQuery):
    """Hourly quantity for a category (e.g. replay=7, attachment bytes=4)."""

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY hour
    ORDER BY hour DESC
    """


class TopOrgsByCategory(OutcomesQuery):
    """Top org/hour pairs by quantity for a category — find who is driving a spike."""

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY hour, org_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """


class TopOrgsByCategoryAggregated(OutcomesQuery):
    """Top orgs by total quantity over the full lookback window."""

    sql = """
    SELECT
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY org_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """


class OrgVolumeOverTime(OutcomesQuery):
    """Hourly quantity for one org and category."""

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        org_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND org_id = {{org_id}}
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY hour, org_id
    ORDER BY hour DESC
    """


class OrgVolumeByReason(OutcomesQuery):
    """Hourly quantity by reason/outcome for one org (abuse limits, usage exceeded, …)."""

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
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY hour, org_id, reason, outcome
    ORDER BY hour DESC, total_quantity DESC
    """


class OrgVolumeByProject(OutcomesQuery):
    """Quantity by project for one org and category."""

    sql = """
    SELECT
        project_id,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND org_id = {{org_id}}
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY project_id
    ORDER BY total_quantity DESC
    LIMIT {{limit}}
    """


class VolumeByOutcomeOverTime(OutcomesQuery):
    """Hourly quantity by outcome (0 accepted … 5 client_discard) for a category."""

    sql = """
    SELECT
        toStartOfHour(timestamp) AS hour,
        outcome,
        sum(quantity) AS total_quantity,
        sum(times_seen) AS total_times_seen
    FROM outcomes_hourly_dist
    WHERE category = {{category}}
        AND timestamp >= {{start_time}}
        AND timestamp < {{end_time}}
    GROUP BY hour, outcome
    ORDER BY hour DESC, total_quantity DESC
    """
