from typing import Sequence

from snuba.datasets.cdc import CdcDataset
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.groupassignees import POSTGRES_TABLE, schema
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.query_processor import QueryProcessor


class GroupAssigneeDataset(CdcDataset):
    """
    This is a clone of sentry_groupasignee table in postgres.

    REMARK: the name in Clickhouse fixes the typo we have in postgres.
    Since the table does not correspond 1:1 to the postgres one anyway
    there is no issue in fixing the name.
    """

    def __init__(self) -> None:
        storage = get_writable_storage("groupassignees")

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            default_control_topic="cdc_control",
            postgres_table=POSTGRES_TABLE,
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
