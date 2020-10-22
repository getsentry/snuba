from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    FixedString,
    Float,
    Nested,
    Nullable,
    String,
    UInt,
)

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines

columns = [
    Column("event_id", FixedString(32)),
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("deleted", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("platform", String([Nullable()])),
    Column("message", String([Nullable()])),
    Column("primary_hash", FixedString(32, [Nullable()])),
    Column("received", DateTime([Nullable()])),
    Column("search_message", String([Nullable()])),
    Column("title", String([Nullable()])),
    Column("location", String([Nullable()])),
    Column("user_id", String([Nullable()])),
    Column("username", String([Nullable()])),
    Column("email", String([Nullable()])),
    Column("ip_address", String([Nullable()])),
    Column("geo_country_code", String([Nullable()])),
    Column("geo_region", String([Nullable()])),
    Column("geo_city", String([Nullable()])),
    Column("sdk_name", String([Nullable()])),
    Column("sdk_version", String([Nullable()])),
    Column("type", String([Nullable()])),
    Column("version", String([Nullable()])),
    Column("offset", UInt(64, [Nullable()])),
    Column("partition", UInt(16, [Nullable()])),
    Column("message_timestamp", DateTime()),
    Column("os_build", String([Nullable()])),
    Column("os_kernel_version", String([Nullable()])),
    Column("device_name", String([Nullable()])),
    Column("device_brand", String([Nullable()])),
    Column("device_locale", String([Nullable()])),
    Column("device_uuid", String([Nullable()])),
    Column("device_model_id", String([Nullable()])),
    Column("device_arch", String([Nullable()])),
    Column("device_battery_level", Float(32, [Nullable()])),
    Column("device_orientation", String([Nullable()])),
    Column("device_simulator", UInt(8, [Nullable()])),
    Column("device_online", UInt(8, [Nullable()])),
    Column("device_charging", UInt(8, [Nullable()])),
    Column("level", String([Nullable()])),
    Column("logger", String([Nullable()])),
    Column("server_name", String([Nullable()])),
    Column("transaction", String([Nullable()])),
    Column("environment", String([Nullable()])),
    Column("sentry:release", String([Nullable()])),
    Column("sentry:dist", String([Nullable()])),
    Column("sentry:user", String([Nullable()])),
    Column("site", String([Nullable()])),
    Column("url", String([Nullable()])),
    Column("app_device", String([Nullable()])),
    Column("device", String([Nullable()])),
    Column("device_family", String([Nullable()])),
    Column("runtime", String([Nullable()])),
    Column("runtime_name", String([Nullable()])),
    Column("browser", String([Nullable()])),
    Column("browser_name", String([Nullable()])),
    Column("os", String([Nullable()])),
    Column("os_name", String([Nullable()])),
    Column("os_rooted", UInt(8, [Nullable()])),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("http_method", String([Nullable()])),
    Column("http_referer", String([Nullable()])),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", String([Nullable()])),
                ("value", String([Nullable()])),
                ("mechanism_type", String([Nullable()])),
                ("mechanism_handled", UInt(8, [Nullable()])),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String([Nullable()])),
                ("filename", String([Nullable()])),
                ("package", String([Nullable()])),
                ("module", String([Nullable()])),
                ("function", String([Nullable()])),
                ("in_app", UInt(8, [Nullable()])),
                ("colno", UInt(32, [Nullable()])),
                ("lineno", UInt(32, [Nullable()])),
                ("stack_level", UInt(16)),
            ]
        ),
    ),
    Column("culprit", String([Nullable()])),
    Column("sdk_integrations", Array(String())),
    Column("modules", Nested([("name", String()), ("version", String())])),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        sample_expr = "cityHash64(toString(event_id))"

        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.EVENTS,
                    version_column="deleted",
                    order_by="(project_id, toStartOfDay(timestamp), %s)" % sample_expr,
                    partition_by="(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))",
                    sample_by=sample_expr,
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="sentry_local"
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="sentry_local",
                    sharding_key="cityHash64(toString(event_id))",
                ),
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="sentry_dist_ro",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="sentry_local",
                    sharding_key="cityHash64(toString(event_id))",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="sentry_dist"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS_RO, table_name="sentry_dist_ro"
            ),
        ]
