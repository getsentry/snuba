from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity


class OutcomesRawDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        outcomes_raw_entity = get_entity(EntityKey.OUTCOMES_RAW)
        super().__init__(entities=[outcomes_raw_entity])
