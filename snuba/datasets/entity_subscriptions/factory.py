from __future__ import annotations

from typing import Generator, Mapping, MutableMapping, Sequence, Type

from snuba import settings
from snuba.datasets.configuration.entity_subscription_builder import (
    build_entity_subscription_from_config,
)
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity_subscriptions.entity_subscription import EntitySubscription
from snuba.utils.config_component_factory import ConfigComponentFactory
from snuba.utils.serializable_exception import SerializableException


class _EntitySubscriptionFactory(
    ConfigComponentFactory[Type[EntitySubscription], EntityKey]
):
    def __init__(self) -> None:
        self._entity_subscription_map: MutableMapping[
            EntityKey, Type[EntitySubscription]
        ] = {}
        self._name_map: MutableMapping[Type[EntitySubscription], EntityKey] = {}
        self.__initialize()

    def __initialize(self) -> None:
        from snuba.datasets.entity_subscriptions.entity_subscription import (
            EventsSubscription,
            GenericMetricsDistributionsSubscription,
            GenericMetricsSetsSubscription,
            MetricsCountersSubscription,
            MetricsSetsSubscription,
            TransactionsSubscription,
        )

        entity_subscription_to_config_path_mapping: Mapping[EntityKey, str] = {
            EntityKey.GENERIC_METRICS_SETS: "snuba/datasets/configuration/generic_metrics/entity_subscriptions/sets.yaml",
            EntityKey.GENERIC_METRICS_DISTRIBUTIONS: "snuba/datasets/configuration/generic_metrics/entity_subscriptions/distributions.yaml",
        }

        self._entity_subscription_map.update(
            {
                EntityKey.EVENTS: EventsSubscription,
                EntityKey.DISCOVER: EntitySubscription,
                EntityKey.TRANSACTIONS: TransactionsSubscription,
                EntityKey.METRICS_COUNTERS: MetricsCountersSubscription,
                EntityKey.METRICS_SETS: MetricsSetsSubscription,
                EntityKey.GENERIC_METRICS_SETS: GenericMetricsSetsSubscription,
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS: GenericMetricsDistributionsSubscription,
            }
        )

        if settings.PREFER_PLUGGABLE_ENTITY_SUBSCRIPTIONS:
            self._entity_subscription_map.update(
                {
                    key: build_entity_subscription_from_config(path)
                    for (
                        key,
                        path,
                    ) in entity_subscription_to_config_path_mapping.items()
                }
            )

        self._name_map = {v: k for k, v in self._entity_subscription_map.items()}

    def iter_all(self) -> Generator[Type[EntitySubscription], None, None]:
        for ent_sub in self._entity_subscription_map.values():
            yield ent_sub

    def all_names(self) -> Sequence[EntityKey]:
        return [name for name in self._entity_subscription_map.keys()]

    def get(self, name: EntityKey) -> Type[EntitySubscription]:
        try:
            return self._entity_subscription_map[name]
        except KeyError as error:
            raise InvalidEntitySubscriptionError(
                f"entity subscription {name!r} does not exist"
            ) from error

    def get_entity_subscription_name(
        self, entity_subscription: Type[EntitySubscription]
    ) -> EntityKey:
        # TODO: This is dumb, the name should just be a property on the entity
        try:
            return self._name_map[entity_subscription]
        except KeyError as error:
            raise InvalidEntitySubscriptionError(
                f"entity subscription {entity_subscription} has no name"
            ) from error


class InvalidEntitySubscriptionError(SerializableException):
    """Exception raised on invalid entity access."""


_ENT_SUB_FACTORY: _EntitySubscriptionFactory | None = None


def _ent_sub_factory() -> _EntitySubscriptionFactory:
    global _ENT_SUB_FACTORY
    if _ENT_SUB_FACTORY is None:
        _ENT_SUB_FACTORY = _EntitySubscriptionFactory()
    return _ENT_SUB_FACTORY


def get_entity_subscription(name: EntityKey) -> Type[EntitySubscription]:
    return _ent_sub_factory().get(name)


def get_entity_subscription_name(
    entity_subscription: Type[EntitySubscription],
) -> EntityKey:
    return _ent_sub_factory().get_entity_subscription_name(entity_subscription)


def get_all_entity_subscription_names() -> Sequence[EntityKey]:
    return _ent_sub_factory().all_names()
