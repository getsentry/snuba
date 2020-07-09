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
    Column("platform", Nullable(String())),
    Column("message", Nullable(String())),
    Column("primary_hash", Nullable(FixedString(32))),
    Column("received", Nullable(DateTime())),
    Column("search_message", Nullable(String())),
    Column("title", Nullable(String())),
    Column("location", Nullable(String())),
    Column("user_id", Nullable(String())),
    Column("username", Nullable(String())),
    Column("email", Nullable(String())),
    Column("ip_address", Nullable(String())),
    Column("geo_country_code", Nullable(String())),
    Column("geo_region", Nullable(String())),
    Column("geo_city", Nullable(String())),
    Column("sdk_name", Nullable(String())),
    Column("sdk_version", Nullable(String())),
    Column("type", Nullable(String())),
    Column("version", Nullable(String())),
    Column("offset", Nullable(UInt(64))),
    Column("partition", Nullable(UInt(16))),
    Column("message_timestamp", DateTime()),
    Column("os_build", Nullable(String())),
    Column("os_kernel_version", Nullable(String())),
    Column("device_name", Nullable(String())),
    Column("device_brand", Nullable(String())),
    Column("device_locale", Nullable(String())),
    Column("device_uuid", Nullable(String())),
    Column("device_model_id", Nullable(String())),
    Column("device_arch", Nullable(String())),
    Column("device_battery_level", Nullable(Float(32))),
    Column("device_orientation", Nullable(String())),
    Column("device_simulator", Nullable(UInt(8))),
    Column("device_online", Nullable(UInt(8))),
    Column("device_charging", Nullable(UInt(8))),
    Column("level", Nullable(String())),
    Column("logger", Nullable(String())),
    Column("server_name", Nullable(String())),
    Column("transaction", Nullable(String())),
    Column("environment", Nullable(String())),
    Column("sentry:release", Nullable(String())),
    Column("sentry:dist", Nullable(String())),
    Column("sentry:user", Nullable(String())),
    Column("site", Nullable(String())),
    Column("url", Nullable(String())),
    Column("app_device", Nullable(String())),
    Column("device", Nullable(String())),
    Column("device_family", Nullable(String())),
    Column("runtime", Nullable(String())),
    Column("runtime_name", Nullable(String())),
    Column("browser", Nullable(String())),
    Column("browser_name", Nullable(String())),
    Column("os", Nullable(String())),
    Column("os_name", Nullable(String())),
    Column("os_rooted", Nullable(UInt(8))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("http_method", Nullable(String())),
    Column("http_referer", Nullable(String())),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", Nullable(String())),
                ("value", Nullable(String())),
                ("mechanism_type", Nullable(String())),
                ("mechanism_handled", Nullable(UInt(8))),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", Nullable(String())),
                ("filename", Nullable(String())),
                ("package", Nullable(String())),
                ("module", Nullable(String())),
                ("function", Nullable(String())),
                ("in_app", Nullable(UInt(8))),
                ("colno", Nullable(UInt(32))),
                ("lineno", Nullable(UInt(32))),
                ("stack_level", UInt(16)),
            ]
        ),
    ),
    Column("culprit", Nullable(String())),
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
                    sharding_key="cityHash64(toString(event_id)))",
                ),
            )
            # TODO: We need to also create the events_ro table here. It
            # needs to be split into it's own storage and storage set first.
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="sentry_dist"
            )
        ]
