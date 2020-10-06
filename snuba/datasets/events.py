from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import EntityKey, get_entity


class EventsDataset(Dataset):
    def __init__(self) -> None:
        events_entity = get_entity(EntityKey.EVENTS)
        super().__init__(default_entity=events_entity)
