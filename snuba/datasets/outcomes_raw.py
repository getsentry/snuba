from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.quota.project_quota_control import ProjectQuotaControl


class OutcomesRawDataset(Dataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self) -> None:
        super().__init__(
            default_entity=EntityKey.OUTCOMES_RAW,
            quota_control=ProjectQuotaControl("project_id"),
        )
