from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey, EntityKeys


class OutcomesRawDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        super().__init__(default_entity=EntityKeys.OUTCOMES_RAW)
