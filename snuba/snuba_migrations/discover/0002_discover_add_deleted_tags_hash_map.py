from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the _tags_hash_map and deleted columns to the merge table so we can use
    the tags optimization, and ensure deleted rows are omitted from queries.
    """

    blocking = False

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("_tags_hash_map", Array(UInt(64))),
                after="tags",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("deleted", UInt(8)),
                after="contexts",
            ),
        ]

    def __backward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column_name="_tags_hash_map",
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column_name="deleted",
            ),
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("discover_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("discover_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("discover_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("discover_dist")
