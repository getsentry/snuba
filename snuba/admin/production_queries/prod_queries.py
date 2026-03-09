from typing import Any, Dict

from flask import Response

from snuba import settings
from snuba.admin.audit_log.query import audit_log
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.query.data_source.projects_finder import ProjectsFinder
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
        dataset_name = body.pop("dataset")
        dataset = get_dataset(dataset_name)
        body["dry_run"] = True
        response = dataset_query(dataset_name, body, Timer("admin"))
        if response.status_code != 200:
            return response

        body["dry_run"] = False
        _validate_projects_in_query(body, dataset, False)
        return dataset_query(dataset_name, body, Timer("admin"))

    return run_query_with_audit(body["query"], user)


def _validate_projects_in_query(body: Dict[str, Any], dataset: Dataset, is_mql: bool) -> None:
    """
    Validates that the projects accessed by the query are allowed to be accessed.
    """
    # In debug, we don't need to validate projects
    if settings.DEBUG and len(settings.ADMIN_ALLOWED_PROD_PROJECTS) == 0:
        return

    if not is_mql:
        request_parts = RequestSchema.build(HTTPQuerySettings).validate(body)
        query = parse_snql_query(request_parts.query["query"], dataset)
        project_ids = ProjectsFinder().visit(query)
    else:
        request_parts = RequestSchema.build(settings_class=HTTPQuerySettings, is_mql=True).validate(
            body
        )
        mql_context = request_parts.query["mql_context"]
        project_ids = set(mql_context["scope"]["project_ids"])

    if project_ids == set():
        raise InvalidQueryException("Missing project ID")

    disallowed_project_ids = project_ids.difference(set(settings.ADMIN_ALLOWED_PROD_PROJECTS))
    if len(disallowed_project_ids) > 0:
        raise InvalidQueryException(
            f"Cannot access the following project ids: {disallowed_project_ids}"
        )


def run_mql_query(body: Dict[str, Any], user: str) -> Response:
    """
    Validates, audit logs, and executes given query.
    """

    @audit_log
    def run_query_with_audit(query: str, user: str) -> Response:
        is_mql = True
        dataset_name = body.pop("dataset")
        dataset = get_dataset(dataset_name)
        body["dry_run"] = True
        response = dataset_query(dataset_name, body, Timer("admin"), is_mql)
        if response.status_code != 200:
            return response

        body["dry_run"] = False
        _validate_projects_in_query(body, dataset, True)
        return dataset_query(dataset_name, body, Timer("admin"), is_mql)

    return run_query_with_audit(body["query"], user)
