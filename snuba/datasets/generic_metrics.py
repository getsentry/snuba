from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey

DEFAULT_GRANULARITY = 60


class GenericMetricsDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                EntityKey.GENERIC_METRICS_SETS,
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
            ]
        )
