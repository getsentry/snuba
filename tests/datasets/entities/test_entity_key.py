import pytest

from snuba.datasets.entities.entity_key import REGISTERED_ENTITY_KEYS, EntityKey
from snuba.datasets.entities.factory import reset_entity_factory


def test_entity_key() -> None:
    reset_entity_factory()
    with pytest.raises(AttributeError):
        EntityKey.NON_EXISTENT_ENTITY
    assert REGISTERED_ENTITY_KEYS == {
        "GENERIC_METRICS_DISTRIBUTIONS": "generic_metrics_distributions",
        "GENERIC_METRICS_SETS": "generic_metrics_sets",
    }
