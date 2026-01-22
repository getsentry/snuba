from abc import ABC
from typing import Sequence

from snuba.clickhouse.columns import UUID, ColumnSet, String, UInt
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.query.data_source.join import ColumnEquivalence, JoinRelationship, JoinType
from snuba.query.processors.logical import LogicalQueryProcessor

EVENTS_SCHEMA = ColumnSet(
    [
        ("event_id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("group_id", UInt(32)),
        ("user_id", UInt(64)),
    ]
)

GROUPS_SCHEMA = ColumnSet(
    [
        ("id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("user_id", UInt(64)),
    ]
)


class FakeEntity(Entity, ABC):
    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return []


class Events(FakeEntity):
    def __init__(self) -> None:
        super().__init__(
            storages=[],
            abstract_column_set=EVENTS_SCHEMA,
            join_relationships={
                "grouped": JoinRelationship(
                    rhs_entity=EntityKey.PROFILES,
                    columns=[("group_id", "id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                # This makes even less sense but self referencing joins are possible
                "self_relationship": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    columns=[("event_id", "event_id")],
                    join_type=JoinType.INNER,
                    equivalences=[],
                ),
            },
            validators=None,
            required_time_column=None,
            subscription_processors=None,
            subscription_validators=None,
        )


class Profiles(FakeEntity):
    def __init__(self) -> None:
        super().__init__(
            storages=[],
            abstract_column_set=GROUPS_SCHEMA,
            join_relationships={
                "events": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    columns=[("id", "group_id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
            },
            validators=None,
            required_time_column=None,
            subscription_processors=None,
            subscription_validators=None,
        )
