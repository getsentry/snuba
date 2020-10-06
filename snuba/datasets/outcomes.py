from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey, get_entity


class OutcomesDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        outcomes_entity = get_entity(EntityKey.OUTCOMES)
        super().__init__(default_entity=outcomes_entity)
