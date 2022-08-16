import threading
from typing import Any

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import (
    get_all_entity_names,
    get_entity,
    get_entity_name,
)
from snuba.datasets.entity import Entity

ENTITY_KEYS = [
    EntityKey.DISCOVER,
    EntityKey.EVENTS,
    EntityKey.GROUPASSIGNEE,
    EntityKey.GROUPEDMESSAGES,
    EntityKey.OUTCOMES,
    EntityKey.OUTCOMES_RAW,
    EntityKey.SESSIONS,
    EntityKey.ORG_SESSIONS,
    EntityKey.TRANSACTIONS,
    EntityKey.DISCOVER_TRANSACTIONS,
    EntityKey.DISCOVER_EVENTS,
    EntityKey.METRICS_SETS,
    EntityKey.METRICS_COUNTERS,
    EntityKey.ORG_METRICS_COUNTERS,
    EntityKey.METRICS_DISTRIBUTIONS,
    EntityKey.PROFILES,
    EntityKey.FUNCTIONS,
    EntityKey.REPLAYS,
    EntityKey.GENERIC_METRICS_SETS,
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
]

# This test should be the first to ensure dataset factory module is has fresh set of objects
def test_get_entity_multithreaded_collision() -> None:
    class GetEntityThread(threading.Thread):
        def test_get_entity_threaded(self) -> None:
            ent_name = EntityKey.EVENTS
            entity = get_entity(ent_name)
            assert isinstance(entity, Entity)
            assert get_entity_name(entity) == ent_name

        def run(self) -> None:
            self.exception = None
            try:
                self.test_get_entity_threaded()
            except Exception as e:
                self.exception = e

        def join(self, *args: Any, **kwargs: Any) -> None:
            threading.Thread.join(self)
            if self.exception:
                raise self.exception

    threads = []
    for _ in range(10):
        thread = GetEntityThread()
        threads.append(thread)
        thread.start()

    for thread in threads:
        try:
            thread.join()
        except Exception as error:
            raise error


def test_get_entity() -> None:
    for ent_name in ENTITY_KEYS:
        entity = get_entity(ent_name)
        assert isinstance(entity, Entity)
        assert get_entity_name(entity) == ent_name


def test_all_names() -> None:
    assert set(get_all_entity_names()) == set(ENTITY_KEYS)
