import logging
from typing import Any, Sequence, Tuple
from unittest.mock import Mock, patch

import pytest

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.check_dangerous import DangerousOperationError
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.connect import get_column_states
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import ModifyColumn, OperationTarget, SqlOperation
from snuba.utils.schemas import Column, String, UInt

logger = logging.getLogger(__name__)


def make_migration(op: SqlOperation) -> ClickhouseNodeMigration:
    class Migration(ClickhouseNodeMigration):
        blocking = False

        def forwards_ops(self) -> Sequence[SqlOperation]:
            return [op]

        def backwards_ops(self) -> Sequence[SqlOperation]:
            return [op]

    return Migration()


@patch.object(SqlOperation, "execute")  # dont actually run the migrations
@pytest.mark.clickhouse_db
class TestDangerousMigration:
    table_name = "test_dangerous_migration"
    connection = None

    @pytest.fixture(scope="class")
    def create_table(self, create_databases: None) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS)
        database = cluster.get_database()
        cls = self.__class__
        cls.connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

        cls.connection.execute(f"DROP TABLE IF EXISTS {database}.{cls.table_name}")
        cls.connection.execute(
            f"CREATE TABLE {database}.{cls.table_name} (name LowCardinality(String), age Nullable(UInt8) ) "
            " ENGINE = MergeTree ORDER BY name"
        )

    def _make_modify_op(
        self, column: Column[MigrationModifiers]
    ) -> Tuple[SqlOperation, Any]:
        op = ModifyColumn(
            StorageSetKey.EVENTS, self.table_name, column, target=OperationTarget.LOCAL
        )
        context: Any = ("migration-4", logger, lambda x: x)
        return op, context

    def _run_modify_migration(self, column: Column[MigrationModifiers]) -> None:
        op, context = self._make_modify_op(column)
        make_migration(op).forwards(context, columns_state_to_check=get_column_states())

    def test_nullable_and_cardinality(
        self, _mock_execute: Mock, create_table: None
    ) -> None:
        col = Column("name", String(Modifiers(low_cardinality=True)))
        self._run_modify_migration(col)

        # changed cardinality
        with pytest.raises(DangerousOperationError):
            col = Column("name", String(Modifiers(low_cardinality=False)))
            self._run_modify_migration(col)

        col = Column("age", UInt(8, Modifiers(nullable=True)))
        self._run_modify_migration(col)

        # changed nullable
        with pytest.raises(DangerousOperationError):
            col = Column("age", UInt(8, Modifiers(nullable=False)))
            self._run_modify_migration(col)

    def test_codec(self, _mock_execute: Mock, create_table: None) -> None:
        col = Column("age", UInt(8, Modifiers(nullable=True, codecs=["Delta", "ZSTD"])))
        self._run_modify_migration(col)

        # Delta alone throws an error
        with pytest.raises(DangerousOperationError):
            col = Column("age", UInt(8, Modifiers(nullable=True, codecs=["Delta"])))
            self._run_modify_migration(col)

    def test_get_column_states(self, _mock_execute: Mock, create_table: None) -> None:
        states = get_column_states()
        assert self.connection is not None
        host, port = self.connection.host, self.connection.port
        assert states[(host, port, self.table_name, "name")] == "LowCardinality(String)"
        assert states[(host, port, self.table_name, "age")] == "Nullable(UInt8)"
