import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Entity, Storage
from snuba.query.logical import EntityQuery, Query, StorageQuery

entity = Entity(
    key=EntityKey("abc"),
    schema=ColumnSet([]),
)

storage = Storage(key=StorageKey("def"))


entity_query = Query(entity)
storage_query = Query(storage)


def test_query_is_query() -> None:
    assert isinstance(entity_query, Query)
    assert isinstance(storage_query, Query)


def test_entity_is_entity() -> None:
    assert isinstance(entity_query, EntityQuery)
    assert not isinstance(entity_query, StorageQuery)


def test_storage_is_storage() -> None:
    assert isinstance(storage_query, StorageQuery)
    assert not isinstance(storage_query, EntityQuery)


def test_from_query() -> None:
    with pytest.raises(AssertionError):
        StorageQuery.from_query(entity_query)

    with pytest.raises(AssertionError):
        EntityQuery.from_query(storage_query)


def test_polymorphism() -> None:
    assert isinstance(StorageQuery.from_query(storage_query), StorageQuery)
    assert isinstance(StorageQuery.from_query(storage_query), Query)
    assert isinstance(EntityQuery.from_query(entity_query), EntityQuery)
    assert isinstance(EntityQuery.from_query(entity_query), Query)
