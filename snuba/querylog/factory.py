from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset_name
from snuba.query.data_source.projects_finder import ProjectsFinder
from snuba.query.logical import Query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.request import Request
from snuba.utils.metrics.timer import Timer


def get_snuba_query_metadata(
    request: Request, dataset: Dataset, timer: Timer
) -> SnubaQueryMetadata:
    start, end = None, None
    entity_name = "unknown"
    if isinstance(request.query, Query):
        entity_key = request.query.get_from_clause().key
        entity = get_entity(entity_key)
        entity_name = entity_key.value
        if entity.required_time_column is not None:
            start, end = get_time_range(request.query, entity.required_time_column)

    return SnubaQueryMetadata(
        request=request,
        start_timestamp=start,
        end_timestamp=end,
        dataset=get_dataset_name(dataset),
        entity=entity_name,
        timer=timer,
        query_list=[],
        projects=ProjectsFinder().visit(request.query),
        snql_anonymized=request.snql_anonymized,
    )
