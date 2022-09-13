from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity


class GroupAssigneeDataset(Dataset):
    """
    This is a clone of sentry_groupasignee table in postgres.

    REMARK: the name in Clickhouse fixes the typo we have in postgres.
    Since the table does not correspond 1:1 to the postgres one anyway
    there is no issue in fixing the name.
    """

    def get_all_entities(self) -> Sequence[Entity]:
        return [
            get_entity(EntityKey.GROUPASSIGNEE),
        ]
