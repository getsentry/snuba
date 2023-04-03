from clickhouse_driver import Client
from typing import Any

client = Client("localhost")


def create_structure() -> None:
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
        PARTITION BY tuple()
        PRIMARY KEY (project_id, timestamp, replay_id)
        ORDER BY (project_id, timestamp, replay_id)
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
        GROUP BY project_id, replay_start_timestamp, replay_id
        ORDER BY project_id, replay_start_timestamp, replay_id
    """
    )
    print("3", resp)


def delete_structure() -> None:
    client.execute("DROP TABLE IF EXISTS replays_simplified")
    client.execute("DROP TABLE IF EXISTS replays_simplified_materialized")
    client.execute("DROP TABLE IF EXISTS replays_simplified_mv")


def insert_data() -> None:
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


def optimize_table() -> None:
    client.execute("OPTIMIZE TABLE replays_simplified_materialized")


def select_data() -> Any:
    resp = client.execute(
        """
        SELECT replay_id, maxMerge(is_archived), sumMerge(len_urls),  arrayFlatten(groupArrayMerge(agg_urls))
        FROM replays_simplified_mv
        GROUP BY replay_id
    """
    )
    return resp


delete_structure()
create_structure()
insert_data()
optimize_table()
print(select_data())


# SELECT project_id, timestamp, replay_id, minMerge(start), maxMerge(end), countMerge(count_segments), countMerge(count_urls) FROM replays_simplified_materialized
