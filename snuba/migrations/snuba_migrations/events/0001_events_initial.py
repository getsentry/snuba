from typing import Sequence

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
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns = [
    Column("event_id", FixedString(32)),
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("deleted", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("platform", String(Modifiers(nullable=True))),
    Column("message", String(Modifiers(nullable=True))),
    Column("primary_hash", FixedString(32, Modifiers(nullable=True))),
    Column("received", DateTime(Modifiers(nullable=True))),
    Column("search_message", String(Modifiers(nullable=True))),
    Column("title", String(Modifiers(nullable=True))),
    Column("location", String(Modifiers(nullable=True))),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("username", String(Modifiers(nullable=True))),
    Column("email", String(Modifiers(nullable=True))),
    Column("ip_address", String(Modifiers(nullable=True))),
    Column("geo_country_code", String(Modifiers(nullable=True))),
    Column("geo_region", String(Modifiers(nullable=True))),
    Column("geo_city", String(Modifiers(nullable=True))),
    Column("sdk_name", String(Modifiers(nullable=True))),
    Column("sdk_version", String(Modifiers(nullable=True))),
    Column("type", String(Modifiers(nullable=True))),
    Column("version", String(Modifiers(nullable=True))),
    Column("offset", UInt(64, Modifiers(nullable=True))),
    Column("partition", UInt(16, Modifiers(nullable=True))),
    Column("message_timestamp", DateTime()),
    Column("os_build", String(Modifiers(nullable=True))),
    Column("os_kernel_version", String(Modifiers(nullable=True))),
    Column("device_name", String(Modifiers(nullable=True))),
    Column("device_brand", String(Modifiers(nullable=True))),
    Column("device_locale", String(Modifiers(nullable=True))),
    Column("device_uuid", String(Modifiers(nullable=True))),
    Column("device_model_id", String(Modifiers(nullable=True))),
    Column("device_arch", String(Modifiers(nullable=True))),
    Column("device_battery_level", Float(32, Modifiers(nullable=True))),
    Column("device_orientation", String(Modifiers(nullable=True))),
    Column("device_simulator", UInt(8, Modifiers(nullable=True))),
    Column("device_online", UInt(8, Modifiers(nullable=True))),
    Column("device_charging", UInt(8, Modifiers(nullable=True))),
    Column("level", String(Modifiers(nullable=True))),
    Column("logger", String(Modifiers(nullable=True))),
    Column("server_name", String(Modifiers(nullable=True))),
    Column("transaction", String(Modifiers(nullable=True))),
    Column("environment", String(Modifiers(nullable=True))),
    Column("sentry:release", String(Modifiers(nullable=True))),
    Column("sentry:dist", String(Modifiers(nullable=True))),
    Column("sentry:user", String(Modifiers(nullable=True))),
    Column("site", String(Modifiers(nullable=True))),
    Column("url", String(Modifiers(nullable=True))),
    Column("app_device", String(Modifiers(nullable=True))),
    Column("device", String(Modifiers(nullable=True))),
    Column("device_family", String(Modifiers(nullable=True))),
    Column("runtime", String(Modifiers(nullable=True))),
    Column("runtime_name", String(Modifiers(nullable=True))),
    Column("browser", String(Modifiers(nullable=True))),
    Column("browser_name", String(Modifiers(nullable=True))),
    Column("os", String(Modifiers(nullable=True))),
    Column("os_name", String(Modifiers(nullable=True))),
    Column("os_rooted", UInt(8, Modifiers(nullable=True))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("http_method", String(Modifiers(nullable=True))),
    Column("http_referer", String(Modifiers(nullable=True))),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", String(Modifiers(nullable=True))),
                ("value", String(Modifiers(nullable=True))),
                ("mechanism_type", String(Modifiers(nullable=True))),
                ("mechanism_handled", UInt(8, Modifiers(nullable=True))),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String(Modifiers(nullable=True))),
                ("filename", String(Modifiers(nullable=True))),
                ("package", String(Modifiers(nullable=True))),
                ("module", String(Modifiers(nullable=True))),
                ("function", String(Modifiers(nullable=True))),
                ("in_app", UInt(8, Modifiers(nullable=True))),
                ("colno", UInt(32, Modifiers(nullable=True))),
                ("lineno", UInt(32, Modifiers(nullable=True))),
                ("stack_level", UInt(16)),
            ]
        ),
    ),
    Column("culprit", String(Modifiers(nullable=True))),
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
