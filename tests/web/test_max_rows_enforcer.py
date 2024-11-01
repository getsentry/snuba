import uuid
from datetime import datetime
from typing import Any, Callable, MutableMapping
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
from tests.datasets.configuration.utils import ConfigurationTest
from tests.helpers import write_unprocessed_events
from tests.test_api import SimpleAPITest


class TestMaxRowsEnforcer(SimpleAPITest, BaseApiTest, ConfigurationTest):
    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        initialize_snuba()
        self.events_storage = get_entity(EntityKey.SEARCH_ISSUES).get_writable_storage()
        assert self.events_storage is not None

        from_clause = Table(
            "search_issues_local_v2",
            ColumnSet([]),
            storage_key=StorageKey.SEARCH_ISSUES,
            # TODO: add allocation policies
            allocation_policies=[],
        )
        self.occurrence_id = str(uuid.uuid4())
        self.query = Query(
            from_clause=from_clause,
            condition=and_cond(
                equals(column("occurrence_id"), literal(self.occurrence_id)),
                equals(column("project_id"), literal(3)),
            ),
            on_cluster=None,
            is_delete=True,
        )

    def _insert_event(self) -> None:
        set_config("read_through_cache.short_circuit", 1)
        now = datetime.now().replace(minute=0, second=0, microsecond=0)

        evt: MutableMapping[str, Any] = dict(
            organization_id=1,
            project_id=3,
            event_id=str(uuid.uuid4().hex),
            group_id=3,
            primary_hash=str(uuid.uuid4().hex),
            datetime=datetime.utcnow().isoformat() + "Z",
            platform="other",
            message="message",
            data={"received": now.timestamp()},
            occurrence_data=dict(
                id=self.occurrence_id,
                type=1,
                issue_title="search me",
                fingerprint=["one", "two"],
                detection_time=now.timestamp(),
            ),
            retention_days=90,
        )

        assert self.events_storage
        write_unprocessed_events(self.events_storage, [evt])

    @pytest.mark.clickhouse_db
    def test_max_row_enforcer_passes(self) -> None:
        self._insert_event()
        _enforce_max_rows(self.query)

    @pytest.mark.clickhouse_db
    @mock.patch(
        "snuba.datasets.storage.ReadableTableStorage.get_deletion_settings",
        return_value=DeletionSettings(
            is_enabled=1,
            tables=["search_issues_local_v2"],
            max_rows_to_delete=0,
            allowed_columns=["project_id", "group_id"],
        ),
    )
    def test_max_row_enforcer_rejects(self, mock: mock.MagicMock) -> None:
        self._insert_event()
        with pytest.raises(TooManyDeleteRowsException):
            _enforce_max_rows(self.query)
