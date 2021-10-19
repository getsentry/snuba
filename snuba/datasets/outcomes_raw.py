from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey


class OutcomesRawDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        super().__init__(default_entity=EntityKey.OUTCOMES_RAW)
