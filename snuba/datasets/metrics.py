from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey


class MetricsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                EntityKey.METRICS_COUNTERS,
                EntityKey.METRICS_DISTRIBUTIONS,
                EntityKey.METRICS_SETS,
                EntityKey.ORG_METRICS_COUNTERS,
            ]
        )
