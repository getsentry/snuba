from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.quota.project_quota_control import ProjectQuotaControl


class OutcomesDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        super().__init__(
            default_entity=EntityKey.OUTCOMES,
            quota_control=ProjectQuotaControl("project_id"),
        )
