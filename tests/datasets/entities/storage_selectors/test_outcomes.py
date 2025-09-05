import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entities.storage_selectors.outcomes import OutcomesStorageSelector
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, OutcomesQuerySettings

OUTCOMES_ENTITY = Entity(
    key=EntityKey("outcomes"),
    schema=get_entity(EntityKey("outcomes")).get_data_model(),
    sample=None,
)
DAILY = get_storage(StorageKey.OUTCOMES_DAILY)
HOURLY = get_storage(StorageKey.OUTCOMES_HOURLY)

TEST_CASES = [
    pytest.param(OutcomesQuerySettings(), HOURLY),
    pytest.param(OutcomesQuerySettings(use_daily=True), DAILY),
    pytest.param(HTTPQuerySettings(), HOURLY),
]


@pytest.mark.parametrize(
    "settings, expected_storage",
    TEST_CASES,
)
def test_storage_selector(
    settings: HTTPQuerySettings,
    expected_storage: Storage,
) -> None:
    """
    Test that we route queries to either hourly or daily outcomes tables
    based on the `use_daily` setting passed through in OutcomesQuerySettings
    If just HTTPQuerySettings, then uses hourly table.
    """
    unimportant_query = Query(from_clause=OUTCOMES_ENTITY)
    connections = get_entity(EntityKey.OUTCOMES).get_all_storage_connections()

    selected_storage = OutcomesStorageSelector().select_storage(
        unimportant_query, settings, connections
    )
    assert selected_storage.storage == expected_storage
