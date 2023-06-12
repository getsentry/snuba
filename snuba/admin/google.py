from abc import ABC
from typing import Optional
from urllib.parse import urlencode

import structlog
from googleapiclient.discovery import Resource, build

logger = structlog.get_logger().bind(module=__name__)


class GoogleAPIWrapper(ABC):
    """
    An abstract class defining an interface for google cloud APIs.
    """


class CloudIdentityAPI:
    """
    An interface for the Google Cloud Identity API.
    """

    def __init__(self) -> None:
        try:
            self.service: Resource = build("cloudidentity", "v1")
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

    def check_group_membership(self, group_email: str, member: str) -> bool:
        try:
            group_resource_name = self._get_group_id(group_email)
            if not group_resource_name:
                return False

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
