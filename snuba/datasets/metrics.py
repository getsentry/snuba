from typing import Sequence

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity

DEFAULT_GRANULARITY = 60


class MetricsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.METRICS_SETS)

    def get_all_entities(self) -> Sequence[Entity]:
        return (
            get_entity(EntityKey.METRICS_COUNTERS),
            get_entity(EntityKey.METRICS_DISTRIBUTIONS),
            get_entity(EntityKey.METRICS_SETS),
            get_entity(EntityKey.ORG_METRICS_COUNTERS),
        )
