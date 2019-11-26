# clickhouse-copier instructions

NOTE: This is pretty manual (though there are scripts included to help certain steps).
In theory we could, in the future, have a tool that handled all of the
complexities here but since a copy is kind of risky I feel like we should
understand the manual steps more before we spend a lot of time attempting to
automate all of this.

---

Create a new table (on the same database cluster or a different one) that has
*the same columns* as the existing table. The copier is only for changing engine
settings, sharding keys, or replication counts. If the data itself must be
changed a full backfill needs to be performed.

## copier-config.xml

Create a `copier-config.xml` file. It is effectively a subset of the
clickhouse-server's `config.xml` except you almost certainly want to log to a
different location.

The only required sections are `<logger>` and `<zookeeper>`. Note that so far
I've chosen to log to the current working directory (I run them as my own user
from `$HOME`).

```xml
<yandex>
    <logger>
        <!-- Possible levels: https://github.com/pocoproject/poco/blob/develop/Foundation/include/Poco/Logger.h#L105 -->
        <level>trace</level>
        <log>copier.log</log>
        <errorlog>copier.err.log</errorlog>
        <size>1000M</size>
        <count>10</count>
        <!-- Default behavior is autodetection (log to console if not daemon mode and is tty) -->
        <!-- <console>1</console> -->
    </logger>

    <zookeeper>
        <node index="0">
            <host>snuba-zookeeper-03b9e33b</host>
            <port>2181</port>
        </node>
        <node index="1">
            <host>snuba-zookeeper-32719002</host>
            <port>2181</port>
        </node>
        <node index="2">
            <host>snuba-zookeeper-4a1e3256</host>
            <port>2181</port>
        </node>
    </zookeeper>
</yandex>
```

## Task description

Create a copier `description` (XML) file using the following template. Note that
you need to do proper XML escaping, a reminder:

```text
"   &quot;
'   &apos;
<   &lt;
>   &gt;
&   &amp;
```

`source_cluster` and `destination_cluster` are self explanatory and map directly
to cluster definitions you'd find in the clickhouse-server `config.xml`.

`engine` should be the *local* table engine you want the destination to use. I
had issues with this working as expected, I highly recommend creating the
destination table(s) yourself manually before starting the job. The job will do
a `CREATE IF NOT EXISTS` which will effectively be a no-op.

`sharding_key` should be the sharding key you intend to use for the
*distributed* table that points at the destination.

`where_condition` can be used if you need to exclude some data from the copy.
Remember that XML escaping applies.

`enabled_partitions` isn't required but ClickHouse developers recommend it. In
the case of a partial copy that was stopped you can remove those partitions from
the list and resume the job. Every partition listed *will be dropped* on the
destination before the copy of that partitions begins, so this lets you fine
tune which partitions will and won't be dropped and copied.

```python
from clickhouse_driver import Client

# note that this assumes every node has the same partitions, which is true in our
# current configuration
c = Client("snuba-storage-0-0")

parts = c.execute("""
    SELECT DISTINCT partition
    FROM system.parts
    WHERE active
      AND table = 'sentry_local'
    ORDER BY partition
    """)

print("\n".join(["<partition>%s</partition>" % p[0].replace("'", "&apos;") for p in parts]))
```

Trim that list to your liking and put it into the `description`.

```xml
<yandex>
    <!-- Configuration of clusters as in an ordinary server config -->
    <remote_servers>
        <source_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>snuba-storage-0-0</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-0-1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-0-2</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>snuba-storage-1-0</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-1-1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-1-2</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>snuba-storage-2-0</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-2-1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-2-2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </source_cluster>

        <destination_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>snuba-storage-0-0</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-0-1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-0-2</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>snuba-storage-1-0</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-1-1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-1-2</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>snuba-storage-2-0</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-2-1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>snuba-storage-2-2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </destination_cluster>
    </remote_servers>

    <!-- How many simultaneously active workers are possible.
         If you run more workers superfluous workers will sleep. -->
    <max_workers>9</max_workers>

    <!-- Setting used to fetch (pull) data from source cluster tables -->
    <settings_pull>
        <readonly>1</readonly>
    </settings_pull>

    <!-- Setting used to insert (push) data to destination cluster tables -->
    <settings_push>
        <readonly>0</readonly>
    </settings_push>

    <!-- Common setting for fetch (pull) and insert (push) operations. The copier process context also uses it.
         They are overlaid by <settings_pull/> and <settings_push/> respectively. -->
    <settings>
        <connect_timeout>3</connect_timeout>
        <!-- Sync insert is set forcibly, leave it here just in case. -->
        <insert_distributed_sync>1</insert_distributed_sync>
    </settings>

    <!-- Copying description of tasks.
         You can specify several table tasks in the same task description (in the same ZooKeeper node),
         and they will be performed sequentially.
    -->
    <tables>
        <!-- A table task that copies one table. -->
        <table_sentry_local>
            <!-- Source cluster name (from the <remote_servers/> section) and tables in it that should be copied -->
            <cluster_pull>source_cluster</cluster_pull>
            <database_pull>default</database_pull>
            <table_pull>sentry_local</table_pull>

            <!-- Destination cluster name and tables in which the data should be inserted -->
            <cluster_push>destination_cluster</cluster_push>
            <database_push>default</database_push>
            <table_push>sentry_local_copy_test1</table_push>

            <!-- Engine of destination tables.
                 If the destination tables have not been created yet, workers create them using column
                 definitions from source tables and the engine definition from here.

                 NOTE: If the first worker starts to insert data and detects that the destination partition
                 is not empty, then the partition will be dropped and refilled. Take this into account if
                 you already have some data in destination tables. You can directly specify partitions that
                 should be copied in <enabled_partitions/>. They should be in quoted format like the partition
                 column in the system.parts table.
            -->
            <engine>
            ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/sentry_local_copy_test1', '{replica}', deleted)
            PARTITION BY (toStartOfDay(timestamp), retention_days)
            ORDER BY (project_id, timestamp, event_id)
            SAMPLE BY intHash32(reinterpretAsInt64(event_id))
            </engine>

            <!-- Sharding key used to insert data to destination cluster -->
            <sharding_key>intHash64(reinterpretAsInt64(event_id))</sharding_key>

            <!-- Optional expression that filter data while pull them from source servers -->
            <!-- <where_condition></where_condition> -->

            <!-- This section specifies partitions that should be copied, other partition will be ignored.
                 Partition names should have the same format as
                 partition column of system.parts table (i.e. a quoted text).
                 Since partition key of source and destination cluster could be different,
                 these partition names specify destination partitions.

                 Note: Although this section is optional (if it omitted, all partitions will be copied),
                 it is strongly recommended to specify the partitions explicitly.
                 If you already have some partitions ready on the destination cluster, they
                 will be removed at the start of the copying, because they will be interpreted
                 as unfinished data from the previous copying.
            -->
            <enabled_partitions>
                <partition>(&apos;2018-04-14 00:00:00&apos;, 90)</partition>
                <!-- and so on... -->
            </enabled_partitions>
        </table_sentry_local>
    </tables>
</yandex>
```

Once the `description` has been completed it will need to be copied into the
ZooKeeper cluster indentified in `copier-config.xml`.

```bash
docker cp description.xml dockerctl_zookeeper_00:/
docker exec -i -t dockerctl_zookeeper_00 /bin/bash
## you may need to create the parent zknodes if they don't exist
# zkCli.sh create /clickhouse ""
# zkCli.sh create /clickhouse/copier ""
## you may need to delete the existing description if one exists
# zkCli.sh delete /clickhouse/copier/description
zkCli.sh create /clickhouse/copier/description "$(cat /description.xml)"
```

## Run the copier(s)

Run as many copier instances as you like. Only up to `max_workers` (from the
`description`) will do work at the same time. They don't seem to use many
resources themselves so I run them on the ClickHouse nodes.

`config-file` is the location of your `copier-config.xml` from above, you'll
need to copy it to each server you run a copier on.

`task-path` is the path in ZooKeeper to use as the base for the copier
instance(s). They will look for the `description` ZNode in that path and use
additional ZNodes in that path to coordinate work.

`base-dir` is where the copier will write local files, I've just been using my
home directory so far.

```bash
clickhouse-copier --config-file=copier-config.xml --task-path=/clickhouse/copier --base-dir .
```

Note that the copier can run in daemon mode, which would mean they just sit and
watch for changes to the `description`, which may be useful in the future.
That said I find running them much easier than configuring and uploading the
`description`.

## Verify

The following script can be used to compare counts between partitions on the
source and destination tables. (If partitions change between source and
destination we'll have to adjust this script to somehow reconcile counts.) For
some reason a few shards didn't seem to copy correctly, so this script (or
something like it) should always be used to verify results and re-run over
failed partitions. The cause of the failures isn't known at this time, we can
investigate when we have time later.

This script requires the `clickhouse_driver` library.

```python
from collections import defaultdict

from clickhouse_driver import Client

# a `Distributed` table for source and destination
old_dist = 'sentry_dist_old'
new_dist = 'sentry_dist_new'

# range of days copied,
# start is <= timestamp
start_day = '2018-04-14'
# end is > timestamp
end_day = '2018-07-18'

count_query = """
    SELECT toStartOfDay(timestamp) AS day, count() AS count
    FROM %%s
    WHERE timestamp >= Cast('%s 00:00:00', 'DateTime') AND timestamp < Cast('%s 00:00:00', 'DateTime')
    GROUP BY day
    ORDER BY day
""" % (start_day, end_day)

c = Client("snuba-storage-0-0")
c.execute("set max_threads = 16")
old_counts = c.execute(count_query % old_dist)
new_counts = c.execute(count_query % new_dist)

joined = defaultdict(dict)

for day, count in old_counts:
    joined[day]['old'] = count

for day, count in new_counts:
    joined[day]['new'] = count

for day, counts in joined.items():
    old = counts.get('old')
    new = counts.get('new')
    day = day.strftime("%Y-%m-%d")
    if not old or not new:
        print((day, 0))
    else:
        similarity = float(new) / old
        if similarity < 0.9999:
            print((day, float(new) / old))
```

Outputs something like this. ~99.9% similarity is because old events have
arrived since the copy happened. Those will catch up once the consumer is
started. Outliers like `2018-05-01` and `2018-07-08` were not copied correctly.
Copy `description` file in ZooKeeper should be adjusted to only include those
failed partitions and then re-run the job to fix them.

```python
('2018-05-01', 0.8355531262971738) # <---
('2018-07-08', 0.4138544968730205) # <---
('2018-07-09', 0.9998954351168708)
('2018-07-10', 0.9992566194276155)
```

Repeat verification and re-run the copy task until all partitions are >= 99.9%
similar.

## Start the consumers

Once the copy is complete you need to start the "realtime" Kafka consumers up
with offsets that pick up where the copy left off.

This script requires the `confluent_kafka` library.

*NOTE:* We tried this script and while it created the consumer group it seemed
to set the offsets too far in the past (like before the earliest offset). Kafka
ships with a tool to set a consumer group to a timestamp that can be used
instead.

```python
from confluent_kafka import Consumer, TopicPartition
from datetime import datetime
import time

group_id = 'sentry_local_copy_test1-consumers'
start_date = datetime(2018, 7, 11)
topic = 'events'
partition_count = 64

consumer_config = {
    'enable.auto.commit': False,
    'bootstrap.servers': '127.0.0.1:9093',
    'group.id': group_id,
    'default.topic.config': {
        'auto.offset.reset': 'error',
    },
}

consumer = Consumer(consumer_config)
timestamp = int(time.mktime(start_date.timetuple()))

topic_partitions = [
    TopicPartition(topic, p, timestamp) for p in xrange(partition_count)
]
topic_partitions = consumer.offsets_for_times(topic_partitions)

consumer.commit(offsets=topic_partitions, asynchronous=False)
```

Start up the consumers and they should pick up at `2018-07-11` (midnight,
exactly where the copy left off).
