import pytest

from snuba.datasets.entities.entity_key import REGISTERED_ENTITY_KEYS, EntityKey


def test_entity_key() -> None:
    with pytest.raises(AttributeError):
        EntityKey.NON_EXISTANT_ENTITY
    assert REGISTERED_ENTITY_KEYS == {}
