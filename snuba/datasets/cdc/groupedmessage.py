from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.quota.project_quota_control import ProjectQuotaControl


class GroupedMessageDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(
            default_entity=EntityKey.GROUPEDMESSAGES,
            quota_control=ProjectQuotaControl(),
        )
