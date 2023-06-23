import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.utils.describer import Description, Property


@pytest.mark.skip(reason="Dataset no longer exists")
def test_entity_describer() -> None:
    entity = get_entity(EntityKey.GROUPASSIGNEE)
    description = entity.describe()

    assert description == Description(
        header=None,
        content=[
            Description(
                header="Entity schema",
                content=[
                    "offset UInt64",
                    "record_deleted UInt8",
                    "project_id UInt64",
                    "group_id UInt64",
                    "date_added Nullable(DateTime)",
                    "user_id Nullable(UInt64)",
                    "team_id Nullable(UInt64)",
                ],
            ),
            Description(
                header="Relationships",
                content=[
                    Description(
                        header="owns",
                        content=[
                            Property("Destination", "events"),
                            Property("Type", "LEFT"),
                            Description(
                                header="Join keys",
                                content=[
                                    "project_id = LEFT.project_id",
                                    "group_id = LEFT.group_id",
                                ],
                            ),
                        ],
                    )
                ],
            ),
        ],
    )
