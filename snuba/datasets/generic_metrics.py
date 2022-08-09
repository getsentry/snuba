from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKeys
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity

DEFAULT_GRANULARITY = 60


class GenericMetricsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKeys.GENERIC_METRICS_SETS)

    def get_all_entities(self) -> Sequence[Entity]:
        return [
            get_entity(EntityKeys.GENERIC_METRICS_SETS),
            get_entity(EntityKeys.GENERIC_METRICS_DISTRIBUTIONS),
        ]
