from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity


class MetricsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                get_entity(EntityKey.METRICS_COUNTERS),
                get_entity(EntityKey.METRICS_DISTRIBUTIONS),
                get_entity(EntityKey.METRICS_SETS),
                get_entity(EntityKey.ORG_METRICS_COUNTERS),
            ]
        )
