from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKeys


class OutcomesDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        super().__init__(default_entity=EntityKeys.OUTCOMES)
