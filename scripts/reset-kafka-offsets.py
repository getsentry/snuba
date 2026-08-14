"""\
This is the nuclear option for resetting Snuba consumer offsets
by using the data in ClickHouse.

Note that all consumers should be stopped before this is run.
"""

import sys

import clickhouse_connect
from confluent_kafka import Consumer, TopicPartition

clickhouse_host = sys.argv[1]  # ex: snuba-query.i.getsentry.net
group_id = sys.argv[2]  # ex: snuba-consumers-3
topic = sys.argv[3]  # ex: 'events'

clickhouse = clickhouse_connect.get_client(host=clickhouse_host)
clickhouse.command("set max_threads = 8")

consumer_config = {
    "enable.auto.commit": False,
    "bootstrap.servers": "127.0.0.1:9093",
    "group.id": group_id,
    "auto.offset.reset": "error",
}

consumer = Consumer(consumer_config)

offsets = clickhouse.query(
    """
    select partition, max(offset)
    from sentry_dist
    where partition is not null
    group by partition
    order by partition
"""
).result_rows

topic_partitions = [TopicPartition(topic, partition=o[0], offset=o[1]) for o in offsets]

consumer.commit(offsets=topic_partitions, asynchronous=False)
