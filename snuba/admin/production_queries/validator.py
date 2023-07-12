import re

from snuba import settings
from snuba.admin.clickhouse.common import InvalidCustomQuery

PROJECT_PATTERNS = {
    "equal": [
        "^(?:project_id\s*=\s*(?P<project_id>\d+))$",
        "^equals\(\(project_id\s+as\s+\w+\),\s*(?P<project_id>\d+)\)$",
        "^equals\(project_id,\s*(?P<project_id>\d+)\)$",
    ],
    "in": [
        "^(?:project_id\s*in\s*\[(?P<project_list>(?:\s*\d+\s*,)*\s*\d+\)])$",
        "^in\(\(project_id\s+as\s+\w+\),\s*\[(?P<project_list>(?:\s*\d+\s*,)*\s*\d+)\]\)$",
        "^in\(project_id,\s*\[(?P<project_list>(?:\s*\d+\s*,)*\s*\d+)\]\)$",
    ],
}


def validate_sql_query(query: str) -> None:
    """
    Validates the query by enforcing a project condition and restricting subquries
    """
    lower = query.lower()
    if lower.count("select") > 1:
        raise InvalidCustomQuery("Subqueries are not allowed")

    where_match = re.search("where", lower)
    if not where_match:
        raise InvalidCustomQuery("Missing condition on project_id")

    where_clause_start = where_match.end()
    where_clause = lower[where_clause_start:].strip()
    conditions = [condition.strip() for condition in where_clause.split(" and ")]

    missing_condition = True
    for condition in conditions:
        if _check_condition(condition):
            missing_condition = False

    if missing_condition:
        raise InvalidCustomQuery("Missing condition on project_id")


def _check_condition(condition: str) -> bool:
    for pattern in PROJECT_PATTERNS["equal"]:
        if equal_match := re.match(pattern, condition):
            project_id = int(equal_match.group("project_id"))
            if project_id not in settings.ADMIN_ALLOWED_PROD_PROJECTS:
                raise InvalidCustomQuery(
                    f"Cannot access the following project ids: {project_id}"
                )

            return True

    for pattern in PROJECT_PATTERNS["in"]:
        if equal_match := re.match(pattern, condition):
            project_ids = set(map(int, equal_match.group("project_list").split(",")))
            disallowed_project_ids = project_ids.difference(
                set(settings.ADMIN_ALLOWED_PROD_PROJECTS)
            )
            if len(disallowed_project_ids) > 0:
                raise InvalidCustomQuery(
                    f"Cannot access the following project ids: {disallowed_project_ids}"
                )

            return True

    return False
