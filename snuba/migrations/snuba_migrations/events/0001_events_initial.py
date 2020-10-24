from typing import List, Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    FixedString,
    Float,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import nullable, MigrationModifiers

columns: List[Column[MigrationModifiers]] = [
    Column("event_id", FixedString(32)),
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("deleted", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("platform", String(nullable())),
    Column("message", String(nullable())),
    Column("primary_hash", FixedString(32, nullable())),
    Column("received", DateTime(nullable())),
    Column("search_message", String(nullable())),
    Column("title", String(nullable())),
    Column("location", String(nullable())),
    Column("user_id", String(nullable())),
    Column("username", String(nullable())),
    Column("email", String(nullable())),
    Column("ip_address", String(nullable())),
    Column("geo_country_code", String(nullable())),
    Column("geo_region", String(nullable())),
    Column("geo_city", String(nullable())),
    Column("sdk_name", String(nullable())),
    Column("sdk_version", String(nullable())),
    Column("type", String(nullable())),
    Column("version", String(nullable())),
    Column("offset", UInt(64, nullable())),
    Column("partition", UInt(16, nullable())),
    Column("message_timestamp", DateTime()),
    Column("os_build", String(nullable())),
    Column("os_kernel_version", String(nullable())),
    Column("device_name", String(nullable())),
    Column("device_brand", String(nullable())),
    Column("device_locale", String(nullable())),
    Column("device_uuid", String(nullable())),
    Column("device_model_id", String(nullable())),
    Column("device_arch", String(nullable())),
    Column("device_battery_level", Float(32, nullable())),
    Column("device_orientation", String(nullable())),
    Column("device_simulator", UInt(8, nullable())),
    Column("device_online", UInt(8, nullable())),
    Column("device_charging", UInt(8, nullable())),
    Column("level", String(nullable())),
    Column("logger", String(nullable())),
    Column("server_name", String(nullable())),
    Column("transaction", String(nullable())),
    Column("environment", String(nullable())),
    Column("sentry:release", String(nullable())),
    Column("sentry:dist", String(nullable())),
    Column("sentry:user", String(nullable())),
    Column("site", String(nullable())),
    Column("url", String(nullable())),
    Column("app_device", String(nullable())),
    Column("device", String(nullable())),
    Column("device_family", String(nullable())),
    Column("runtime", String(nullable())),
    Column("runtime_name", String(nullable())),
    Column("browser", String(nullable())),
    Column("browser_name", String(nullable())),
    Column("os", String(nullable())),
    Column("os_name", String(nullable())),
    Column("os_rooted", UInt(8, nullable())),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("http_method", String(nullable())),
    Column("http_referer", String(nullable())),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", String(nullable())),
                ("value", String(nullable())),
                ("mechanism_type", String(nullable())),
                ("mechanism_handled", UInt(8, nullable())),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String(nullable())),
                ("filename", String(nullable())),
                ("package", String(nullable())),
                ("module", String(nullable())),
                ("function", String(nullable())),
                ("in_app", UInt(8, nullable())),
                ("colno", UInt(32, nullable())),
                ("lineno", UInt(32, nullable())),
                ("stack_level", UInt[MigrationModifiers](16)),
            ]
        ),
    ),
    Column("culprit", String(nullable())),
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
