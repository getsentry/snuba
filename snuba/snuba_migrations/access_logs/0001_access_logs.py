from typing import Sequence

from snuba.clickhouse.columns import (
    Column,
    Date,
    DateTime,
    FixedString,
    Float,
    IPv4,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

"""
CREATE TABLE default.access_log
(
    `_time` DateTime CODEC(DoubleDelta, LZ4),
    `_date` Date DEFAULT toDate(_time),
    `_ms` UInt32,
    `body_bytes_sent` UInt32 CODEC(ZSTD(1)),
    `status` UInt32 CODEC(T64, ZSTD(1)),
    `upstream_bytes_received` UInt32 CODEC(ZSTD(1)),
    `upstream_response_length` UInt32 CODEC(ZSTD(1)),
    `request_length` UInt32 CODEC(ZSTD(1)),
    `project_id` UInt64,
    `request` String CODEC(LZ4HC(0)) TTL _date + toIntervalDay(90),
    `request_uri` String CODEC(LZ4) TTL _date + toIntervalDay(30),
    `request_uri_path` LowCardinality(String) MATERIALIZED path(request_uri),
    `request_time` Float32 CODEC(Gorilla, LZ4) TTL _date + toIntervalDay(1),
    `upstream_connect_time` Float32 CODEC(Gorilla, LZ4),
    `upstream_response_time` Float32 CODEC(LZ4) TTL _date + toIntervalDay(1),
    `upstream_response_time_ms` UInt32 MATERIALIZED CAST(round(upstream_response_time * 1000, 0), 'UInt32') CODEC(T64, LZ4),
    `request_id` FixedString(32) CODEC(LZ4) TTL _date + toIntervalDay(1),
    `http_referrer` String CODEC(LZ4HC(0)) TTL _date + toIntervalDay(30),
    `remote_user` LowCardinality(String),
    `host` LowCardinality(String) CODEC(ZSTD(1)),
    `http_host` LowCardinality(String),
    `http_user_agent` LowCardinality(String) CODEC(LZ4),
    `request_completion` LowCardinality(String),
    `request_method` LowCardinality(String),
    `ssl_protocol` LowCardinality(String),
    `ssl_cipher` LowCardinality(String) DEFAULT '',
    `ssl_server_name` LowCardinality(String) DEFAULT '',
    `statsd_path` LowCardinality(String),
    `remote_addr` IPv4 CODEC(ZSTD(1)),
    `request_time_ms` UInt32 MATERIALIZED CAST(round(request_time * 1000, 0), 'UInt32') CODEC(ZSTD(1)),
    `upstream_name` LowCardinality(String) DEFAULT CAST('', 'LowCardinality(String)'),
    `upstream_remote_address` String CODEC(LZ4) TTL _date + toIntervalDay(1),
    INDEX minmax_status status TYPE minmax GRANULARITY 4,
    INDEX minmax_project_id project_id TYPE minmax GRANULARITY 4
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/clicktail/access_log', 'chalet')
PARTITION BY _date
ORDER BY (statsd_path, request_uri_path, _date, _time)
TTL _date + toIntervalDay(400)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 1000000000
"""


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False
    index_granularity = "8192"
    min_bytes_for_wide_part = "1000000000"
    local_table_name = "access_logs_local"
    dist_table_name = "access_logs_dist"
    columns: Sequence[Column[Modifiers]] = [
        Column("_time", DateTime(Modifiers(codecs=["DoubleDelta", "LZ4"]))),
        Column("_date", Date(Modifiers(default="toDate(_time)"))),
        Column("_ms", UInt(32)),
        Column("body_bytes_sent", UInt(32, Modifiers(codecs=["ZSTD(1)"]))),
        Column("status", UInt(32, Modifiers(codecs=["T64", "ZSTD(1)"]))),
        Column("upstream_bytes_received", UInt(32, Modifiers(codecs=["ZSTD(1)"]))),
        Column("upstream_response_length", UInt(32, Modifiers(codecs=["ZSTD(1)"]))),
        Column("request_length", UInt(32, Modifiers(codecs=["ZSTD(1)"]))),
        Column("project_id", UInt(64)),
        Column(
            "request",
            String(Modifiers(codecs=["LZ4HC(0)"], ttl="_date + toIntervalDay(90)")),
        ),
        Column(
            "request_uri",
            String(Modifiers(codecs=["LZ4"], ttl="_date + toIntervalDay(30)")),
        ),
        Column(
            "request_uri_path",
            String(Modifiers(low_cardinality=True, materialized="path(request_uri)")),
        ),
        Column(
            "request_time",
            Float(
                32, Modifiers(codecs=["Gorilla", "LZ4"], ttl="_date + toIntervalDay(1)")
            ),
        ),
        Column(
            "upstream_connect_time", Float(32, Modifiers(codecs=["Gorilla", "LZ4"]))
        ),
        Column(
            "upstream_response_time",
            Float(32, Modifiers(codecs=["LZ4"], ttl="_date + toIntervalDay(1)")),
        ),
        Column(
            "upstream_response_time_ms",
            UInt(
                32,
                Modifiers(
                    codecs=["T64", "LZ4"],
                    materialized="CAST(round(upstream_response_time * 1000, 0), 'UInt32')",
                ),
            ),
        ),
        Column(
            "request_id",
            FixedString(32, Modifiers(codecs=["LZ4"], ttl="_date + toIntervalDay(1)")),
        ),
        Column(
            "http_referrer",
            String(Modifiers(codecs=["LZ4HC(0)"], ttl="_date + toIntervalDay(30)")),
        ),
        Column("remote_user", String(Modifiers(low_cardinality=True))),
        Column("host", String(Modifiers(low_cardinality=True, codecs=["ZSTD(1)"]))),
        Column("http_host", String(Modifiers(low_cardinality=True))),
        Column(
            "http_user_agent", String(Modifiers(low_cardinality=True, codecs=["LZ4"]))
        ),
        Column("request_completion", String(Modifiers(low_cardinality=True))),
        Column("request_method", String(Modifiers(low_cardinality=True))),
        Column("ssl_protocol", String(Modifiers(low_cardinality=True))),
        Column("ssl_cipher", String(Modifiers(low_cardinality=True, default="''"))),
        Column(
            "ssl_server_name", String(Modifiers(low_cardinality=True, default="''"))
        ),
        Column("statsd_path", String(Modifiers(low_cardinality=True))),
        Column("remote_addr", IPv4(Modifiers(codecs=["ZSTD(1)"]))),
        Column(
            "request_time_ms",
            UInt(
                32,
                Modifiers(
                    codecs=["ZSTD(1)"],
                    materialized="CAST(round(request_time * 1000, 0), 'UInt32')",
                ),
            ),
        ),
        Column(
            "upstream_name",
            String(
                Modifiers(
                    low_cardinality=True, default="CAST('', 'LowCardinality(String)')"
                )
            ),
        ),
        Column(
            "upstream_remote_address",
            String(Modifiers(codecs=["LZ4"], ttl="_date + toIntervalDay(1)")),
        ),
    ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.ACCESS_LOGS,
                table_name=self.local_table_name,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.ACCESS_LOGS,
                    order_by="(statsd_path, request_uri_path, _date, _time)",
                    partition_by="_date",
                    settings={
                        "index_granularity": self.index_granularity,
                        "min_bytes_for_wide_part": self.min_bytes_for_wide_part,
                    },
                    ttl="_date + toIntervalDay(400)",
                ),
                columns=self.columns,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.ACCESS_LOGS,
                table_name=self.local_table_name,
                index_expression="status",
                index_name="minmax_status",
                index_type="minmax",
                granularity=4,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.ACCESS_LOGS,
                table_name=self.local_table_name,
                index_expression="project_id",
                index_name="minmax_project_id",
                index_type="minmax",
                granularity=4,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.ACCESS_LOGS,
                table_name=self.local_table_name,
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.ACCESS_LOGS,
                table_name=self.dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name,
                    sharding_key="request_id",
                ),
                columns=self.columns,
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.ACCESS_LOGS,
                table_name=self.dist_table_name,
            )
        ]
