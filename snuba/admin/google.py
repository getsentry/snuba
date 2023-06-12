from typing import Optional
from urllib.parse import urlencode

import structlog
from googleapiclient.discovery import Resource, build

logger = structlog.get_logger().bind(module=__name__)


class CloudIdentityAPI:
    """
    A class for interfacing with the Google Cloud Identity API.
    """

    def __init__(self, service: Resource = None) -> None:
        try:
            self.service: Resource = (
                service if service else build("cloudidentity", "v1")
            )
        except Exception as e:
            logger.exception(e)

    def _get_group_id(self, group_email: str) -> Optional[str]:
        try:
            query_params = f"groupKey.id={group_email}"
            request = self.service.groups().lookup()
            request.uri += "&" + query_params
            response = request.execute()
            if "error" in response:
                logger.exception(
                    f"An HTTP error occured when fetching group id for email {group_email}.",
                    google_api_error=response["error"],
                )
                return None

            return str(response["name"])
        except Exception as e:
            logger.exception(e)

        return None

    def _check_transitive_membership(
        self, group_resource_name: str, member: str
    ) -> bool:
        try:
            query_params = urlencode({"query": "member_key_id == '{}'".format(member)})
            request = (
                self.service.groups()
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

    def check_group_membership(self, group_email: str, member: str) -> bool:
        group_resource_name = self._get_group_id(group_email)
        if group_resource_name:
            return self._check_transitive_membership(
                group_resource_name=group_resource_name, member=member
            )

        return False
