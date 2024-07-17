import typing
import uuid

from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.query import ProcessableQuery
from snuba.query.data_source.simple import Table
from snuba.query.exceptions import MaxRowsEnforcerException
from snuba.query.logical import Query as LogicalQuery
from snuba.query.parsing import ParsingContext
from snuba.request import DeleteRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query



def max_rows_enforcer(delete_request: DeleteRequest) -> None:
    query = delete_request.query
    where_clause = delete_request.where_clause

    storage_key = query.get_from_clause().key
    storage = get_storage(storage_key)

    deletion_settings = storage.get_deletion_settings()

    query_to_count_rows = (
        f"MATCH ({storage.get_storage_key().value}) SELECT COUNT() {where_clause}"
    )
    # query_to_count_rows = "MATCH (search_issues) SELECT COUNT() WHERE timestamp >= toDateTime('2024-07-17T21:04:34') AND timestamp < toDateTime('2024-07-17T21:10:34') AND project_id = 1"
    print(query_to_count_rows)
    print(str(uuid.uuid4()))
    print(f"MATCH ({storage.get_storage_key().value}) SELECT COUNT() {where_clause}")
    # assert False
    request_body = {
        "query": query_to_count_rows,
        "tenant_ids": delete_request.tenant_ids,
    }

    _, query_result = parse_and_run_query(
        body=request_body, timer=Timer("parse_and_run_query")
    )
    rows_to_delete = query_result.result["data"][0]["COUNT()"]

    print(rows_to_delete)
    if rows_to_delete > deletion_settings.max_rows_to_delete:
        raise MaxRowsEnforcerException(
            f"Query wants to delete {rows_to_delete} rows, but the maximum allowed is {deletion_settings.max_rows_to_delete}"
        )
