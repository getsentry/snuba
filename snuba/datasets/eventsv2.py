from datetime import timedelta
from typing import Sequence

from snuba.datasets import TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.join import (
    JoinConditionExpression,
    JoinCondition,
    JoinedSchema,
    JoinStructure,
    JoinType,
    SchemaJoinedSource,
    SubJoinSource,
)
from snuba.query.extensions import PERFORMANCE_EXTENSION_SCHEMA, PROJECT_EXTENSION_SCHEMA
from snuba.query.schema import GENERIC_QUERY_SCHEMA
from snuba.request import RequestSchema
from snuba.schemas import get_time_series_extension_properties


class EventsV2(TimeSeriesDataset):
    def __init__(self) -> None:
        grouped_message = get_dataset("groupedmessage")
        events = get_dataset("events")

        join_structure = JoinStructure(
            left_source=SchemaJoinedSource(
                "groups",
                grouped_message.get_dataset_schemas().get_read_schema(),
            ),
            right_source=SchemaJoinedSource(
                "events",
                events.get_dataset_schemas().get_read_schema(),
            ),
            mapping=[
                JoinCondition(
                    left=JoinConditionExpression(table_alias="groups", column="project_id"),
                    right=JoinConditionExpression(table_alias="events", column="project_id"),
                ),
                JoinCondition(
                    left=JoinConditionExpression(table_alias="groups", column="id"),
                    right=JoinConditionExpression(table_alias="events", column="group_id"),
                ),
            ],
            join_type=JoinType.LEFT,
        )

        schema = JoinedSchema(join_structure)
        dataset_schemas = DatasetSchemas(
            read_schema=schema,
            write_schema=None,
        )
        super().__init__(
            dataset_schemas=dataset_schemas,
            processor=None,
            default_topic=None,
            time_group_columns={
                'events.time': 'events.timestamp',
            },
            timestamp_column='events.timestamp',
        )

    # TODO: Implement column_expr by delegating to the dependent
    # datasets

    def get_query_schema(self) -> RequestSchema:
        return RequestSchema(GENERIC_QUERY_SCHEMA, {
            'performance': PERFORMANCE_EXTENSION_SCHEMA,
            'project': PROJECT_EXTENSION_SCHEMA,
            'timeseries': get_time_series_extension_properties(
                default_granularity=3600,
                default_window=timedelta(days=5),
            ),
        })

    def get_prewhere_keys(self) -> Sequence[str]:
        return ['events.event_id', 'events.project_id']
