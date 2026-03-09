from datetime import datetime
from typing import Any, Callable
from unittest import mock

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.core.initialize import initialize_snuba
from snuba.datasets.deletion_settings import DeletionSettings
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.exceptions import TooManyDeleteRowsException
from snuba.state import set_config
from snuba.web.delete_query import _enforce_max_rows
from tests.base import BaseApiTest
from tests.web.rpc.v1.test_utils import write_eap_item


class TestMaxRowsEnforcer(BaseApiTest):
    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        initialize_snuba()
        self.events_storage = get_entity(EntityKey.EAP_ITEMS).get_writable_storage()
        assert self.events_storage is not None

        from_clause = Table(
            "eap_items_1_local",
            ColumnSet([]),
            storage_key=StorageKey.EAP_ITEMS,
            allocation_policies=[],
        )
        self.query = Query(
            from_clause=from_clause,
            condition=and_cond(
                equals(column("organization_id"), literal(1)),
                equals(column("project_id"), literal(1)),
            ),
            on_cluster=None,
            is_delete=True,
        )

    def _insert_event(self) -> None:
        set_config("read_through_cache.short_circuit", 1)
        now = datetime.now().replace(minute=0, second=0, microsecond=0)

        write_eap_item(
            start_timestamp=now,
            raw_attributes={"test_attribute": "test_value"},
            count=1,
        )

    @pytest.mark.eap
    @pytest.mark.redis_db
    def test_max_row_enforcer_passes(self) -> None:
        self._insert_event()
        _enforce_max_rows(self.query)

    @pytest.mark.eap
    @pytest.mark.redis_db
    @mock.patch(
        "snuba.datasets.storage.ReadableTableStorage.get_deletion_settings",
        return_value=DeletionSettings(
            is_enabled=1,
            tables=["eap_items_1_local"],
            max_rows_to_delete=0,
            allowed_columns=["project_id", "organization_id"],
        ),
    )
    def test_max_row_enforcer_rejects(self, mock: mock.MagicMock) -> None:
        self._insert_event()
        with pytest.raises(TooManyDeleteRowsException):
            _enforce_max_rows(self.query)

    @pytest.mark.eap
    @pytest.mark.redis_db
    @mock.patch(
        "snuba.datasets.storage.ReadableTableStorage.get_deletion_settings",
        return_value=DeletionSettings(
            is_enabled=1,
            tables=["eap_items_1_local"],
            max_rows_to_delete=0,
            allowed_columns=["project_id", "organization_id"],
        ),
    )
    def test_bypass_enforce_max_rows(self, mock: mock.MagicMock) -> None:
        set_config("enforce_max_rows_to_delete", 0)
        self._insert_event()
        _enforce_max_rows(self.query)
