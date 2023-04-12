from datetime import datetime, timezone
from typing import Any

from clickhouse_driver import Client

from snuba.consumers.types import KafkaMessageMetadata
from tests.datasets.test_replays_processor import ReplayEvent

client = Client("localhost")


event_timestamp = datetime.now(tz=timezone.utc)
meta = KafkaMessageMetadata(offset=0, partition=0, timestamp=event_timestamp)


message = ReplayEvent(
    replay_id="e5e062bf2e1d4afd96fd2f90b6770431",
    replay_type="session",
    event_hash=None,
    error_sample_rate=0.5,
    session_sample_rate=0.5,
    title="/organizations/:orgId/issues/",
    error_ids=["36e980a9c6024cde9f5d089f15b83b5f"],
    trace_ids=[
        "36e980a9c6024cde9f5d089f15b83b5f",
        "8bea4461d8b944f393c15a3cb1c4169a",
    ],
    segment_id=0,
    timestamp=int(event_timestamp.timestamp()),
    replay_start_timestamp=int(event_timestamp.timestamp()),
    platform="python",
    dist="",
    urls=["http://127.0.0.1:8001"],
    is_archived=False,
    user_name="me",
    user_id="232",
    user_email="test@test.com",
    os_name="iOS",
    os_version="16.2",
    browser_name="Chrome",
    browser_version="103.0.38",
    device_name="iPhone 11",
    device_brand="Apple",
    device_family="iPhone",
    device_model="iPhone",
    ipv4="127.0.0.1",
    ipv6=None,
    environment="prod",
    release="34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
    sdk_name="sentry.python",
    sdk_version="0.9.0",
)


columns = [
    "replay_id",
    "replay_start_timestamp",
    "timestamp",
    "replay_type",
    "error_sample_rate",
    "session_sample_rate",
    "event_hash",
    "segment_id",
    "trace_ids",
    "title",
    "url",
    "urls",
    "is_archived",
    "error_ids",
    "project_id",
    "platform",
    "environment",
    "release",
    "dist",
    "ip_address_v4",
    "ip_address_v6",
    "user",
    "user_id",
    "user_name",
    "user_email",
    "os_name",
    "os_version",
    "browser_name",
    "browser_version",
    "device_name",
    "device_brand",
    "device_family",
    "device_model",
    "sdk_name",
    "sdk_version",
    "tags.key",
    "tags.value",
    "click_node_id",
    "click_tag",
    "click_id",
    "click_class",
    "click_text",
    "click_role",
    "click_alt",
    "click_testid",
    "click_aria_label",
    "click_title",
    "retention_days",
    "partition",
    "offset",
]


local_table = """
CREATE TABLE replays_simple
(
    `replay_id` UUID,
    `replay_type` LowCardinality(Nullable(String)),
    `error_sample_rate` Nullable(Float64),
    `session_sample_rate` Nullable(Float64),
    `event_hash` UUID,
    `segment_id` Nullable(UInt16),
    `trace_ids` Array(UUID),
    `_trace_ids_hashed` Array(UInt64) MATERIALIZED arrayMap(t -> cityHash64(t), trace_ids),
    `title` Nullable(String),
    `url` Nullable(String),
    `urls` Array(String),
    `is_archived` Nullable(UInt8),
    `error_ids` Array(UUID),
    `_error_ids_hashed` Array(UInt64) MATERIALIZED arrayMap(t -> cityHash64(t), error_ids),
    `project_id` UInt64,
    `timestamp` DateTime,
    `replay_start_timestamp` DateTime, -- should be nullable
    `platform` LowCardinality(String),
    `environment` LowCardinality(Nullable(String)),
    `release` Nullable(String),
    `dist` Nullable(String),
    `ip_address_v4` Nullable(IPv4),
    `ip_address_v6` Nullable(IPv6),
    `user` Nullable(String),
    `user_id` Nullable(String),
    `user_name` Nullable(String),
    `user_email` Nullable(String),
    `os_name` Nullable(String),
    `os_version` Nullable(String),
    `browser_name` Nullable(String),
    `browser_version` Nullable(String),
    `device_name` Nullable(String),
    `device_brand` Nullable(String),
    `device_family` Nullable(String),
    `device_model` Nullable(String),
    `sdk_name` Nullable(String),
    `sdk_version` Nullable(String),
    `tags.key` Array(String),
    `tags.value` Array(String),
    `click_node_id` UInt32 DEFAULT 0,
    `click_tag` LowCardinality(String) DEFAULT '',
    `click_id` String DEFAULT '',
    `click_class` Array(String),
    `click_text` String DEFAULT '',
    `click_role` LowCardinality(String) DEFAULT '',
    `click_alt` String DEFAULT '',
    `click_testid` String DEFAULT '',
    `click_aria_label` String DEFAULT '',
    `click_title` String DEFAULT '',
    `retention_days` UInt16,
    `partition` UInt16,
    `offset` UInt64,
    INDEX bf_trace_ids_hashed _trace_ids_hashed TYPE bloom_filter GRANULARITY 1,
    INDEX bf_error_ids_hashed _error_ids_hashed TYPE bloom_filter GRANULARITY 1,
    INDEX bf_replay_id replay_id TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY (retention_days, toMonday(timestamp))
ORDER BY (project_id, toStartOfDay(timestamp), cityHash64(replay_id), event_hash)
TTL timestamp + toIntervalDay(retention_days)
SETTINGS index_granularity = 8192
"""


destination_table = """
CREATE TABLE replays_materialized (
    `project_id` UInt64,
    `replay_id` UUID,
    `replay_start_timestamp` DateTime,
    `start` AggregateFunction(min, DateTime),
    `end` AggregateFunction(max, DateTime),
    `os_name` AggregateFunction(any, Nullable(String)),
    `count_segments` AggregateFunction(count, Nullable(UInt16)),
    `agg_urls` AggregateFunction(groupArray, Array(String)),
    `len_urls` AggregateFunction(sum, UInt64),
    `is_archived` AggregateFunction(max, Nullable(UInt8))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toStartOfDay(replay_start_timestamp)
PRIMARY KEY (project_id, replay_start_timestamp, replay_id)
ORDER BY (project_id, replay_start_timestamp, replay_id)
"""

matview = """
CREATE MATERIALIZED VIEW replays_materialized_mv TO replays_materialized AS
SELECT
    project_id,
    replay_start_timestamp,
    replay_id,
    minState(replay_start_timestamp) as start,
    maxState(timestamp) as end,
    anyState(os_name) as os_name,
    countState(segment_id) as count_segments,
    groupArrayState(urls) as agg_urls,
    sumState(length(arrayFlatten(urls))) as len_urls,
    maxState(is_archived) as is_archived
FROM replays_simple
GROUP BY project_id, replay_start_timestamp, replay_id
ORDER BY project_id, replay_start_timestamp, replay_id
"""


def create_structure() -> None:
    print("Create local:")
    print(client.execute(local_table))
    print("Create dest:")
    print(client.execute(destination_table))
    print("Create matview:")
    print(client.execute(matview))


def delete_structure() -> None:
    client.execute("DROP TABLE IF EXISTS replays_simple")
    client.execute("DROP TABLE IF EXISTS replays_materialized")
    client.execute("DROP TABLE IF EXISTS replays_materialized_mv")


def encode_value(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, list):
        return f"[{', '.join([encode_value(v) for v in value])}]"
    elif isinstance(value, tuple):
        return f"({', '.join([encode_value(v) for v in value])})"
    elif value is None:
        return "NULL"
    elif isinstance(value, datetime):
        ret, _ = value.isoformat().split("+", 1)
        if "." in ret:
            ret, _ = ret.split(".", 1)
        return f"'{ret}'"
    else:
        return f"'{value}'"


def insert_data() -> None:
    event = message.build_result(meta)
    values = []
    for c in columns:
        value = event.get(c)
        if c == "is_archived":
            value = 0 if value is None else value
        values.append(encode_value(value))

    print("VALUES", {c: v for c, v in zip(columns, values)})
    # values = [event.get(c) for c in columns]
    template = f"INSERT INTO replays_simple ({', '.join(columns)}) VALUES ({', '.join(values)})"
    print("Inserting:", template)
    print(client.execute(template))

    # client.execute(
    #     "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03eba', 1, [], 30, 1)"
    # )
    # client.execute(
    #     "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03eba', 2, ['w', 'y', 'z'], 30, 0)"
    # )
    # client.execute(
    #     "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03ebb', 0, ['x', 'y'], 30, 0)"
    # )
    # client.execute(
    #     "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03ebb', 1, [], 30, 0)"
    # )
    # client.execute(
    #     "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03ebb', 2, ['w', 'y', 'z'], 30, 0)"
    # )


def optimize_table() -> None:
    client.execute("OPTIMIZE TABLE replays_simplified_materialized")


old_query = """
SELECT
	(project_id AS _snuba_project_id),
	(replaceAll(toString(replay_id), '-', '') AS _snuba_replay_id),
	(arrayMap((trace_id -> (replaceAll(toString(trace_id), '-', '') AS _snuba_trace_id)), groupUniqArrayArray((trace_ids AS _snuba_trace_ids))) AS _snuba_traceIds),
	(arrayMap((error_id -> (replaceAll(toString(error_id), '-', '') AS _snuba_error_id)), groupUniqArrayArray((error_ids AS _snuba_error_ids))) AS _snuba_errorIds),
	(min((replay_start_timestamp AS _snuba_replay_start_timestamp)) AS _snuba_started_at),
	(max((timestamp AS _snuba_timestamp)) AS _snuba_finished_at),
	(dateDiff('second', _snuba_started_at, _snuba_finished_at) AS _snuba_duration),
	(arrayFlatten(arraySort((urls, sequence_id -> identity(sequence_id)), arrayMap((url_tuple -> tupleElement(url_tuple, 2)), (groupArray(((segment_id AS _snuba_segment_id), (urls AS _snuba_urls))) AS _snuba_agg_urls)), arrayMap((url_tuple -> tupleElement(url_tuple, 1)), _snuba_agg_urls))) AS _snuba_urls_sorted),
	(count(_snuba_segment_id) AS _snuba_count_segments),
	(sum(length(_snuba_error_ids)) AS _snuba_count_errors),
	(sum(length(_snuba_urls)) AS _snuba_count_urls),
	(notEmpty(groupArray((is_archived AS _snuba_is_archived))) AS _snuba_isArchived),
	(floor(greatest(1, least(10, intDivOrZero(plus(multiply(_snuba_count_errors, 25), multiply(_snuba_count_urls, 5)), 10)))) AS _snuba_activity),
	(groupUniqArray((release AS _snuba_release)) AS _snuba_releases),
	(arrayElement(groupArray(1)(replay_type), 1) AS _snuba_replay_type),
	(arrayElement(groupArray(1)(platform), 1) AS _snuba_platform),
	(arrayElement(groupArray(1)((environment AS _snuba_environment)), 1) AS _snuba_agg_environment),
	(arrayElement(groupArray(1)(dist), 1) AS _snuba_dist),
	(arrayElement(groupArray(1)(user_id), 1) AS _snuba_user_id),
	(arrayElement(groupArray(1)(user_email), 1) AS _snuba_user_email),
	(arrayElement(groupArray(1)(user_name), 1) AS _snuba_user_name),
	(IPv4NumToString(arrayElement(groupArray(1)((ip_address_v4 AS _snuba_ip_address_v4)), 1)) AS _snuba_user_ip),
	(arrayElement(groupArray(1)(os_name), 1) AS _snuba_os_name),
	(arrayElement(groupArray(1)(os_version), 1) AS _snuba_os_version),
	(arrayElement(groupArray(1)(browser_name), 1) AS _snuba_browser_name),
	(arrayElement(groupArray(1)(browser_version), 1) AS _snuba_browser_version),
	(arrayElement(groupArray(1)(device_name), 1) AS _snuba_device_name),
	(arrayElement(groupArray(1)(device_brand), 1) AS _snuba_device_brand),
	(arrayElement(groupArray(1)(device_family), 1) AS _snuba_device_family),
	(arrayElement(groupArray(1)(device_model), 1) AS _snuba_device_model),
	(arrayElement(groupArray(1)(sdk_name), 1) AS _snuba_sdk_name),
	(arrayElement(groupArray(1)(sdk_version), 1) AS _snuba_sdk_version),
	(groupArrayArray((tags.key AS `_snuba_tags.key`)) AS _snuba_tk),
	(groupArrayArray((tags.value AS `_snuba_tags.value`)) AS _snuba_tv)
FROM replays_dist
WHERE in(_snuba_project_id, [5208367])
AND less(_snuba_timestamp, toDateTime('2023-03-20T21:24:17', 'Universal'))
AND greaterOrEquals(_snuba_timestamp, toDateTime('2022-12-20T21:24:17', 'Universal'))
AND equals(_snuba_replay_id, 'cbb0382ab1b849f3af95efd0b5f152f8')
GROUP BY _snuba_project_id, _snuba_replay_id
HAVING equals(min(_snuba_segment_id), 0)
AND less(_snuba_finished_at, toDateTime('2023-03-20T21:24:17', 'Universal'))
AND equals(_snuba_isArchived, 0)
LIMIT 1000 OFFSET 0
"""

new_query = f"""
SELECT
	project_id,
	replay_id,
	--(arrayMap((trace_id -> (replaceAll(toString(trace_id), '-', '') AS _snuba_trace_id)), groupUniqArrayArray((trace_ids AS _snuba_trace_ids))) AS _snuba_traceIds),
	--(arrayMap((error_id -> (replaceAll(toString(error_id), '-', '') AS _snuba_error_id)), groupUniqArrayArray((error_ids AS _snuba_error_ids))) AS _snuba_errorIds),
	minMerge(start) AS started_at,
	maxMerge(end) AS ended_at,
	dateDiff('second', started_at, ended_at) AS duration,
	--(arrayFlatten(arraySort((urls, sequence_id -> identity(sequence_id)), arrayMap((url_tuple -> tupleElement(url_tuple, 2)), (groupArray(((segment_id AS _snuba_segment_id), (urls AS _snuba_urls))) AS _snuba_agg_urls)), arrayMap((url_tuple -> tupleElement(url_tuple, 1)), _snuba_agg_urls))) AS _snuba_urls_sorted),
	countMerge(count_segments) AS _snuba_count_segments,
	--(sum(length(_snuba_error_ids)) AS _snuba_count_errors),
	sumMerge(len_urls) AS _snuba_len_urls,
	maxMerge(is_archived) AS _snuba_is_archived,
	--(floor(greatest(1, least(10, intDivOrZero(plus(multiply(_snuba_count_errors, 25), multiply(_snuba_count_urls, 5)), 10)))) AS _snuba_activity),
	--(groupUniqArray((release AS _snuba_release)) AS _snuba_releases),
	--(arrayElement(groupArray(1)(replay_type), 1) AS _snuba_replay_type),
	--(arrayElement(groupArray(1)(platform), 1) AS _snuba_platform),
	--(arrayElement(groupArray(1)((environment AS _snuba_environment)), 1) AS _snuba_agg_environment),
	--(arrayElement(groupArray(1)(dist), 1) AS _snuba_dist),
	--(arrayElement(groupArray(1)(user_id), 1) AS _snuba_user_id),
	--(arrayElement(groupArray(1)(user_email), 1) AS _snuba_user_email),
	--(arrayElement(groupArray(1)(user_name), 1) AS _snuba_user_name),
	--(IPv4NumToString(arrayElement(groupArray(1)((ip_address_v4 AS _snuba_ip_address_v4)), 1)) AS _snuba_user_ip),
	anyMerge(os_name) as _snuba_os_name
	--(arrayElement(groupArray(1)(os_version), 1) AS _snuba_os_version),
	--(arrayElement(groupArray(1)(browser_name), 1) AS _snuba_browser_name),
	--(arrayElement(groupArray(1)(browser_version), 1) AS _snuba_browser_version),
	--(arrayElement(groupArray(1)(device_name), 1) AS _snuba_device_name),
	--(arrayElement(groupArray(1)(device_brand), 1) AS _snuba_device_brand),
	--(arrayElement(groupArray(1)(device_family), 1) AS _snuba_device_family),
	--(arrayElement(groupArray(1)(device_model), 1) AS _snuba_device_model),
	--(arrayElement(groupArray(1)(sdk_name), 1) AS _snuba_sdk_name),
	--(arrayElement(groupArray(1)(sdk_version), 1) AS _snuba_sdk_version),
	--(groupArrayArray((tags.key AS `_snuba_tags.key`)) AS _snuba_tk),
	--(groupArrayArray((tags.value AS `_snuba_tags.value`)) AS _snuba_tv)
FROM replays_materialized
WHERE in(project_id, [1])
AND less(replay_start_timestamp, (now() + 3600))
AND greaterOrEquals(replay_start_timestamp, toDateTime('2023-01-01'))
AND equals(replay_id, toUUID('e5e062bf2e1d4afd96fd2f90b6770431'))
GROUP BY project_id, replay_id
HAVING _snuba_is_archived = 0
--HAVING equals(min(_snuba_segment_id), 0)
--AND less(_snuba_finished_at, toDateTime('2023-03-20T21:24:17', 'Universal'))
--AND equals(_snuba_isArchived, 0)
LIMIT 1000 OFFSET 0
"""


def select_data() -> Any:
    resp = client.execute(new_query)
    #     """
    #     SELECT replay_id, maxMerge(is_archived), sumMerge(len_urls),  arrayFlatten(groupArrayMerge(agg_urls))
    #     FROM replays_simplified_mv
    #     GROUP BY replay_id
    # """
    return resp


delete_structure()
create_structure()
insert_data()
optimize_table()
print("SELECTED:", select_data())


def create_structure_simple() -> None:
    resp = client.execute(
        """
        CREATE TABLE replays_simplified (
            `project_id` UInt64,
            `replay_start_timestamp` DateTime,
            `timestamp` DateTime,
            `replay_id` UUID,
            `segment_id` UInt64,
            `urls` Array(String),
            `retention_days` UInt8,
            `is_archived` UInt8
        )
        ENGINE = ReplacingMergeTree
        PARTITION BY (retention_days, toMonday(timestamp))
        ORDER BY (project_id, toStartOfDay(timestamp), cityHash64(replay_id), segment_id)
    """
    )
    print("1", resp)
    resp = client.execute(
        """
        CREATE TABLE replays_simplified_materialized (
            `project_id` UInt64,
            `timestamp` DateTime,
            `replay_id` UUID,
            `start` AggregateFunction(min, DateTime),
            `end` AggregateFunction(max, DateTime),
            `count_segments` AggregateFunction(count, UInt64),
            `agg_urls` AggregateFunction(groupArray, Array(String)),
            `len_urls` AggregateFunction(sum, UInt64),
            `is_archived` AggregateFunction(max, UInt8)
        )
        ENGINE = AggregatingMergeTree()
        PARTITION BY tuple(toStartOfDay(end))
        PRIMARY KEY (project_id, start, replay_id)
        ORDER BY (project_id, start, replay_id)
    """
    )
    print("2", resp)
    resp = client.execute(
        """
        CREATE MATERIALIZED VIEW replays_simplified_mv TO replays_simplified_materialized AS
        SELECT
            project_id,
            replay_start_timestamp as timestamp,
            replay_id,
            minState(timestamp) as start,
            maxState(timestamp) as end,
            countState(segment_id) as count_segments,
            groupArrayState(urls) as agg_urls,
            sumState(length(arrayFlatten(urls))) as len_urls,
            maxState(is_archived) as is_archived
        FROM replays_simplified
        GROUP BY project_id, replay_id
        ORDER BY project_id, start, replay_id
    """
    )
    print("3", resp)


def delete_structure_simplified() -> None:
    client.execute("DROP TABLE IF EXISTS replays_simplified")
    client.execute("DROP TABLE IF EXISTS replays_simplified_materialized")
    client.execute("DROP TABLE IF EXISTS replays_simplified_mv")


def insert_data_simplified() -> None:
    client.execute(
        "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03eba', 0, ['x', 'y'], 30, 0)"
    )
    client.execute(
        "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03eba', 1, [], 30, 1)"
    )
    client.execute(
        "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03eba', 2, ['w', 'y', 'z'], 30, 0)"
    )
    client.execute(
        "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03ebb', 0, ['x', 'y'], 30, 0)"
    )
    client.execute(
        "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03ebb', 1, [], 30, 0)"
    )
    client.execute(
        "INSERT INTO replays_simplified (project_id, replay_start_timestamp, timestamp, replay_id, segment_id, urls, retention_days, is_archived) VALUES (1, '2023-03-27T15:37:30', '2023-03-27T15:37:30', '48c9ffa8-1e0f-4e13-af6c-5a22e3d03ebb', 2, ['w', 'y', 'z'], 30, 0)"
    )
    # INSERT INTO replays_simplified_mv VALUES (1, '48c9ffa8-1e0f-4e13-af6c-5a22e3d03eba', '2023-03-27T15:37:30', '2023-03-27T15:37:30', 1, 3, ['a', 'b', 'c'])


# SELECT project_id, timestamp, replay_id, minMerge(start), maxMerge(end), countMerge(count_segments), countMerge(count_urls) FROM replays_simplified_materialized
