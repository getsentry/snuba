from urllib.parse import urlencode

import structlog
from googleapiclient.discovery import Resource

logger = structlog.get_logger().bind(module=__name__)


def get_group_id(service: Resource, group_email: str) -> str:
    try:
        query_params = f"groupKey.id={group_email}"
        request = service.groups().lookup()
        request.uri += "&" + query_params
        response = request.execute()
        if "error" in response:
            logger.exception(
                f"An HTTP error occured when fetching group id for email {group_email}.",
                google_api_error=response["error"],
            )

        return str(response.get("name", ""))
    except Exception as e:
        logger.exception(e)

    return ""


def check_transitive_membership(
    service: Resource, group_resource_name: str, member: str
) -> bool:
    try:
        query_params = urlencode({"query": "member_key_id == '{}'".format(member)})
        request = (
            service.groups()
            .memberships()
            .checkTransitiveMembership(parent=group_resource_name)
        )
        request.uri += "&" + query_params
        response = request.execute()
        if "error" in response:
            logger.exception(
                f"An HTTP error occured when checking if user {member} is a member of group {group_resource_name}",
                google_api_error=response["error"],
            )
        return bool(response.get("hasMembership", False))
    except Exception as e:
        logger.exception(e)

    return False
