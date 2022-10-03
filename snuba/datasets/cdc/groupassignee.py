from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey


class GroupAssigneeDataset(Dataset):
    """
    This is a clone of sentry_groupasignee table in postgres.

    REMARK: the name in Clickhouse fixes the typo we have in postgres.
    Since the table does not correspond 1:1 to the postgres one anyway
    there is no issue in fixing the name.
    """

    def __init__(self) -> None:
        super().__init__(
            all_entities=[
                EntityKey.GROUPASSIGNEE,
            ]
        )
