from typing import Any, List, Set

from snuba import settings
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def _validate_projects_in_query(project_ids: List[int]) -> None:
    if settings.DEBUG and len(settings.ADMIN_ALLOWED_PROD_PROJECTS) == 0:
        return
    allowed_projects: Set[int] = set(settings.ADMIN_ALLOWED_PROD_PROJECTS)
    query_projects: Set[int] = set(project_ids)
    if len(query_projects - allowed_projects) > 0:
        raise BadSnubaRPCRequestException(
            f"Project IDs {query_projects} are not allowed"
        )


def _validate_org_ids_in_query(org_id: int) -> None:
    if settings.DEBUG and len(settings.ADMIN_ALLOWED_ORG_IDS) == 0:
        return
    if org_id not in settings.ADMIN_ALLOWED_ORG_IDS:
        raise BadSnubaRPCRequestException(f"Organization ID {org_id} is not allowed")


def validate_request_meta(request_proto: Any) -> None:
    if not hasattr(request_proto, "meta"):
        raise BadSnubaRPCRequestException("Missing request meta")
    meta = request_proto.meta
    org_id = meta.organization_id
    project_ids = list(meta.project_ids)
    _validate_org_ids_in_query(org_id)
    _validate_projects_in_query(project_ids)
