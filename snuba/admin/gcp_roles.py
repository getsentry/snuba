from collections import defaultdict
from typing import List, MutableMapping

import googleapiclient.discovery

# not sure if both of these are needed...
from google.oauth2 import service_account

from snuba.settings import ADMIN_GCP_PROJECT_ID

GCP_USER_ROLES: MutableMapping[str, List[str]] = defaultdict(list)

ROLE_PREFIX = "roles/clickhouseMigrations."


def set_google_roles() -> None:
    """
    Using the GCP cloud resource manager API, request the list
    of bindings (roles to users) and use those mappings to populate
    GCP_USER_ROLES for the project specified by the GCP_PROJECT_ID.

    Every call to this function will refresh the GCP_USER_ROLES.
    """
    # is this needed? can we get creds a diff way?
    credentials = service_account.Credentials.from_service_account_file(
        filename="search-and-storage-7de40f318ea6.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    service = googleapiclient.discovery.build(
        "cloudresourcemanager", "v1", credentials=credentials
    )

    # https://cloud.google.com/resource-manager/reference/rest/v1/projects/getIamPolicy
    # will return an instance of a gcp policy, example payload:
    # https://cloud.google.com/resource-manager/reference/rest/Shared.Types/Policy
    response = (
        service.projects()
        .getIamPolicy(resource=ADMIN_GCP_PROJECT_ID, body={})
        .execute()
    )

    GCP_USER_ROLES.clear()
    for binding in response["bindings"]:
        if not binding["role"].startswith(ROLE_PREFIX):
            # we don't care about any non-migrations roles
            continue
        role = binding["role"].split(ROLE_PREFIX)[1]
        for member in binding["members"]:
            _, email = member.split(":")
            # TODO: handle group email e.g. team-sns@sentry.io
            GCP_USER_ROLES[email].append(role)
