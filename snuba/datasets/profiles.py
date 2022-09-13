from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity


class ProfilesDataset(Dataset):
    def get_all_entities(self) -> Sequence[Entity]:
        return [
            get_entity(EntityKey.PROFILES),
        ]
