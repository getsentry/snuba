from typing import Any, Dict

from flask import Response

from snuba import settings
from snuba.admin.audit_log.query import audit_log
from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import InvalidQueryException
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer
from snuba.web.views import dataset_query


def run_snql_query(body: Dict[str, Any], user: str) -> Response:
    """
    Validates, audit logs, and executes given query.
    """

    @audit_log
    def run_query_with_audit(query: str, user: str) -> Response:
        dataset = get_dataset(body.pop("dataset"))
        body["dry_run"] = True
        response = dataset_query(dataset, body, Timer("admin"))
        if response.status_code != 200:
            return response

        body["dry_run"] = False
        _validate_projects_in_query(body, dataset)
        return dataset_query(dataset, body, Timer("admin"))

    return run_query_with_audit(body["query"], user)


def run_sql_query(query: str, storage_name: str, user: str) -> ClickhouseResult:
    @audit_log
    def run_query_with_audit(query: str, user: str) -> ClickhouseResult:
        validate_ro_query(query)
        connection = get_ro_query_node_connection(
            storage_name, ClickhouseClientSettings.PROD_QUERY
        )

        query_result = connection.execute(query=query, with_column_types=True)
        return query_result

    return run_query_with_audit(query, user)


def _validate_projects_in_query(body: Dict[str, Any], dataset: Dataset) -> None:
    request_parts = RequestSchema.build(HTTPQuerySettings).validate(body)
    query = parse_snql_query(request_parts.query["query"], dataset)[0]
    project_ids = get_object_ids_in_query_ast(query, "project_id")
    if project_ids is None:
        raise InvalidQueryException("Missing project ID")

    disallowed_project_ids = project_ids.difference(
        set(settings.ADMIN_ALLOWED_PROD_PROJECTS)
    )
    if len(disallowed_project_ids) > 0:
        raise InvalidQueryException(
            f"Cannot access the following project ids: {disallowed_project_ids}"
        )
